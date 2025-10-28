import datetime
import time
import requests
import json
import logging
from cachetools import cached, TTLCache

import humanize

from marshmallow import ValidationError
from sqlalchemy.sql import except_, func
from sqlalchemy.exc import IntegrityError

from mchub.models.cloud.cloud_manager import CloudManager

from .magic_castle_configuration import MagicCastleConfiguration
from .cluster_status_code import ClusterStatusCode

from ..terraform_cloud import TerraformCloudRunORM
from ..terraform.terraform_state import TerraformState
from ..terraform.terraform_plan_parser import TerraformPlanParser
from ..cloud.dns_manager import DnsManager
from ..cloud.project import Project
from ..puppet.provisioning_manager import ProvisioningManager, MAX_PROVISIONING_TIME

from ...configuration.magic_castle import (
    MAIN_TERRAFORM_FILENAME,
    TERRAFORM_STATE_FILENAME,
    MAGIC_CASTLE_PATH,
)
from ...configuration.env import CLUSTERS_PATH

from ...exceptions.invalid_usage_exception import (
    ClusterNotFoundException,
    ClusterExistsException,
    InvalidUsageException,
    BusyClusterException,
    PlanNotCreatedException,
)
from ...exceptions.server_exception import (
    PlanException,
    TerraformCloudException,
)

from ...database import db

from ...services.terraform_cloud_api import get_terraform_cloud
from ...services.github_api import get_github_storage


TERRAFORM_PLAN_BINARY_FILENAME = "terraform_plan"
TERRAFORM_APPLY_LOG_FILENAME = "terraform_apply.log"
TERRAFORM_PLAN_LOG_FILENAME = "terraform_plan.log"


class MagicCastleORM(db.Model):
    __tablename__ = "magiccastle"
    id = db.Column(db.Integer, primary_key=True)
    hostname = db.Column(db.String(256), unique=True, nullable=False)

    tfcloud_workspace = db.Column(db.String(256))
    tfcloud_run = db.relationship(
        "TerraformCloudRunORM",
        back_populates="magic_castle",
        cascade="all, delete-orphan",
        uselist=False,
    )

    status = db.Column(db.Enum(ClusterStatusCode), default=ClusterStatusCode.NOT_FOUND)
    created = db.Column(db.DateTime(), default=func.now())
    expiration_date = db.Column(db.String(32))
    config = db.Column(db.PickleType())
    applied_config = db.Column(db.PickleType())
    tf_state = db.Column(db.PickleType())
    project_id = db.Column(db.Integer, db.ForeignKey("project.id"))
    project = db.relationship(
        "Project",
        back_populates="magic_castles",
        uselist=False,
        cascade_backrefs=False,
    )


@cached(cache=TTLCache(maxsize=1024, ttl=10))
def get_tf_status_cache(workspace_id):
    tf = get_terraform_cloud()
    return tf.get_lastest_run_status(workspace_id)


class MagicCastle:
    """
    Magic Castle is the class that manages everything related to the state of a Magic Castle cluster.
    It is responsible for building, modifying and destroying the cluster using Terraform.
    It is also used to get the state of the cluster and the cloud resources available.

    Note: In this class, the database connection is recreated everytime the database must be accessed
    to avoid using the same connection in multiple threads (which doesn't work with sqlite).
    """

    __slots__ = ["orm"]

    def __init__(self, orm=None):
        if orm:
            self.orm = orm
        else:
            self.orm = MagicCastleORM(
                status=ClusterStatusCode.NOT_FOUND,
                config={},
                tfcloud_run=TerraformCloudRunORM(),
            )

    @property
    def hostname(self):
        return self.orm.hostname

    @property
    def domain(self):
        return self.config.domain

    @property
    def tfcloud_workspace(self):
        return self.orm.tfcloud_workspace

    @property
    def tfcloud_run(self):
        return self.orm.tfcloud_run

    @property
    def cloud_id(self):
        return self.orm.project.id

    @property
    def project(self):
        return self.orm.project

    @property
    def expiration_date(self):
        return self.orm.expiration_date

    @property
    def age(self):
        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        delta = now - self.orm.created
        return humanize.naturaldelta(delta)

    @property
    def config(self):
        return self.orm.config

    @config.setter
    def config(self, value):
        self.orm.config = value

    @property
    def applied_config(self):
        return self.orm.applied_config

    def set_configuration(self, configuration: dict):
        logging.debug(f"Call <{self.__class__.__name__}>:set_configuration")

        expect_tf_changes = False
        self.orm.expiration_date = configuration.pop("expiration_date", None)
        cloud_id = configuration.pop("cloud")["id"]

        if self.orm.project is None or self.orm.project.id != cloud_id:
            self.orm.project = db.session.get(Project, cloud_id)
            expect_tf_changes = True
        try:
            config = MagicCastleConfiguration(self.orm.project.provider, configuration)
        except ValidationError as err:
            raise InvalidUsageException(
                f"The magic castle configuration could not be parsed.\nError: {err.messages}"
            )
        if self.config != config:
            self.config = config
            self.orm.hostname = f"{self.config.cluster_name}.{self.config.domain}"
            expect_tf_changes = True
        return expect_tf_changes

    @property
    def status(self) -> ClusterStatusCode:
        # Update status from Terraform Cloud
        try:
            run_id, tf_status, is_destroy = get_tf_status_cache(
                self.orm.tfcloud_workspace
            )
        except TerraformCloudException as e:
            logging.error(f"Error on {self.orm.tfcloud_workspace}, error={e.message}")
            return self.orm.status

        # New run detected
        if run_id != self.tfcloud_run.run_id:
            self.orm.tfcloud_run = TerraformCloudRunORM(run_id=run_id)

        # Update the state only if the new run is started
        if tf_status is not None and is_destroy is not None:
            self.status = ClusterStatusCode.from_tfcloudstatus(tf_status, is_destroy)

        tf = get_terraform_cloud()
        # Fetch lastest plan if currently empty
        if not self.plan:
            plan = tf.get_run_plan_log_json(run_id)
            if plan is not None:
                self.plan = plan
                logging.info(f"Plan Updated for {run_id=}")

        # Get the apply log
        if self.plan and not self.apply_url:
            apply_url = tf.get_run_apply_log(run_id)
            logging.info(f"Update apply log for {run_id=}")
            self.apply_url = apply_url

        if self.orm.status == ClusterStatusCode.PROVISIONING_RUNNING:
            now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            if ProvisioningManager.check_online(self.hostname):
                self.status = ClusterStatusCode.PROVISIONING_SUCCESS
            elif MAX_PROVISIONING_TIME < (now - self.orm.created).total_seconds():
                self.status = ClusterStatusCode.PROVISIONING_ERROR
        elif self.orm.status == ClusterStatusCode.DESTROY_SUCCESS:
            self.delete()
            return ClusterStatusCode.DESTROY_SUCCESS

        db.session.commit()
        return self.orm.status

    @status.setter
    def status(self, status: ClusterStatusCode):
        if status != self.orm.status:
            self.orm.status = status
            db.session.commit()

            # Log cluster status updates for log analytics
            print(
                json.dumps(
                    {
                        "hostname": self.hostname,
                        "status": self.orm.status,
                    }
                ),
                flush=True,
            )

    @property
    def plan(self) -> dict:
        return self.orm.tfcloud_run.plan

    @plan.setter
    def plan(self, plan: dict):
        self.orm.tfcloud_run.plan = plan

    @property
    def apply_url(self) -> str:
        return self.orm.tfcloud_run.apply_log_url

    @apply_url.setter
    def apply_url(self, apply_url: str):
        self.orm.tfcloud_run.apply_log_url = apply_url

    def get_progress(self):
        if self.apply_url and self.plan:
            res = requests.get(self.apply_url)
            apply_log = ""
            if res.status_code == 200:
                apply_log = res.text

            return TerraformPlanParser.get_done_changes(self.plan, apply_log)

    @property
    def state(self):
        return {
            **(self.applied_config if self.applied_config else self.config),
            "hostname": self.hostname,
            "status": self.status,
            "freeipa_passwd": self.freeipa_passwd,
            "age": self.age,
            "expiration_date": self.expiration_date,
            "cloud": {"name": self.project.name, "id": self.project.id},
        }

    @property
    def tf_state(self):
        return self.orm.tf_state

    @property
    def freeipa_passwd(self):
        if self.tf_state is not None:
            return self.tf_state.freeipa_passwd
        else:
            return None

    @property
    def allocated_resources(self):
        if self.is_busy:
            raise BusyClusterException

        if self.tf_state is not None:
            return dict(
                pre_allocated_instance_count=self.tf_state.instance_count,
                pre_allocated_ram=self.tf_state.ram,
                pre_allocated_cores=self.tf_state.cores,
                pre_allocated_volume_count=self.tf_state.volume_count,
                pre_allocated_volume_size=self.tf_state.volume_size,
            )
        else:
            return dict(
                pre_allocated_instance_count=0,
                pre_allocated_ram=0,
                pre_allocated_cores=0,
                pre_allocated_volume_count=0,
                pre_allocated_volume_size=0,
            )

    @property
    def is_busy(self):
        return self.status in [
            ClusterStatusCode.PLAN_RUNNING,
            ClusterStatusCode.BUILD_RUNNING,
            ClusterStatusCode.DESTROY_RUNNING,
        ]

    @property
    def found(self):
        return self.status != ClusterStatusCode.NOT_FOUND

    def plan_creation(self, data):
        logging.debug(f"Call <{type(self).__name__}>:plan_creation")

        self.set_configuration(data)
        db.session.add(self.orm)
        try:
            db.session.commit()
        except IntegrityError:
            raise ClusterExistsException

        github_repo_fullname = get_github_storage().create_repo(
            self.hostname, self.project.github_template
        )

        workspace_name = github_repo_fullname.split("/")[-1]

        tf = get_terraform_cloud()
        workspace_id = tf.create_workspace(
            workspace_name, github_repo_fullname, self.orm.project.tfcloud_project_id
        )

        logging.info(
            f"{self.hostname}: terraformcloud workspace=<{workspace_id}> created"
        )

        # Write the main terraform file to storage backend
        try:
            var_tf = self.config.get_var_tf()
            github_commit = get_github_storage().write(var_tf, self.hostname)
        except Exception as error:
            self.delete()
            raise PlanException(
                "Could not write variables.tf on the storage backend.",
                additional_details=f"hostname: {self.hostname}, error: {error}",
            )
        logging.info(
            f"{self.hostname}: New commit <{github_commit}> on repo <{github_repo_fullname}>"
        )

        #
        self.orm.tfcloud_workspace = workspace_id

        self.status = ClusterStatusCode.PLAN_RUNNING
        db.session.commit()

    def plan_modification(self, data):
        logging.debug(f"Call <{self.__class__.__name__}>:plan_modification")

        if not self.found:
            raise ClusterNotFoundException
        if self.is_busy:
            raise BusyClusterException

        config_changed = self.set_configuration(data)

        # Check if main_file has changed before writing
        # and planning a change, some modifications may
        # only be reflected in the database and do not
        # require a plan.
        # Add an exception if the cluster is stuck in a destroy error
        if config_changed or self.status == ClusterStatusCode.DESTROY_ERROR:
            try:
                var_tf = self.config.get_var_tf()
                get_github_storage().write(var_tf, self.hostname)
            except Exception as error:
                self.delete()
                raise PlanException(
                    "Could not write variables.tf on the storage backend.",
                    additional_details=f"hostname: {self.hostname}, error: {error}",
                )
            self.status = ClusterStatusCode.PLAN_RUNNING
            db.session.commit()

    def plan_destruction(self):
        logging.debug(f"Call <{self.__class__.__name__}:plan_destruction>")
        tf = get_terraform_cloud()
        run_id = tf.destroy_run(self.orm.tfcloud_workspace)
        logging.info(
            f"{self.hostname}: Apply destroy on workspace_id={self.orm.tfcloud_workspace} with run_id={run_id}"
        )
        self.status = ClusterStatusCode.DESTROY_RUNNING
        db.session.commit()

    def create_plan(self):
        logging.debug(f"Call <{self.__class__.__name__}:create_plan>")
        raise NotImplementedError

    def apply(self):
        logging.debug(f"Call <{self.__class__.__name__}:apply>")
        # raise NotImplementedError

    def delete(self):
        db.session.delete(self.orm)
        db.session.commit()
