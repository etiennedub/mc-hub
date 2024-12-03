import datetime
import time
import requests
import json
import logging

from mchub.models.magic_castle.terraform_cloud_status import TFCloudStatusCode

# logging.basicConfig(level=logging.DEBUG)

from ...configuration import get_config

from requests.api import request

import humanize

from os import path, environ, mkdir, remove, scandir, rename, symlink
from subprocess import run, CalledProcessError
from shutil import rmtree
from threading import Thread

from marshmallow import ValidationError
from sqlalchemy.sql import func
from sqlalchemy.exc import IntegrityError

from mchub.models.cloud.cloud_manager import CloudManager

from github import Github
from github import Auth

from .magic_castle_configuration import MagicCastleConfiguration
from .cluster_status_code import ClusterStatusCode
from .plan_type import PlanType

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


TERRAFORM_PLAN_BINARY_FILENAME = "terraform_plan"
TERRAFORM_APPLY_LOG_FILENAME = "terraform_apply.log"
TERRAFORM_PLAN_LOG_FILENAME = "terraform_plan.log"


class GithubStorage:
    def __init__(self):
        config = get_config()
        self.organization = config["github_organization"]

        auth = Auth.Token(config["github_token"])
        self.github = Github(auth=auth)

    def _get_repo_name(self, hostname):
        import hashlib

        hash_object = hashlib.sha256(hostname.encode())
        hashed_name = hash_object.hexdigest()[:10]
        repo_name = f"mchub-{hashed_name}"
        return repo_name

    def create_repo(self, hostname):
        repo_name = self._get_repo_name(hostname)

        repo_description = f"mchub repo for unique_name '{hostname}'"

        org = self.github.get_organization(self.organization)

        try:
            repo = org.get_repo(repo_name)
        except Exception as err:
            print(err)
            # Repository does not exist, create it
            repo = org.create_repo(
                name=repo_name, description=repo_description, private=True
            )

        return f"{self.organization}/{repo_name}"

    def write(self, tf_data, hostname, filename="terraform.tfvars.json"):
        # Check if the file exists in the repository
        repo_name = self._get_repo_name(hostname)
        org = self.github.get_organization(self.organization)
        repo = org.get_repo(repo_name)

        tf_str = json.dumps(tf_data)

        try:
            file = repo.get_contents(filename)
            # Update the file if it exists
            commit = repo.update_file(
                path=file.path,
                message=f"Update {filename} content",
                content=tf_str,
                sha=file.sha,  # Required for updating
            )
        except Exception as err:
            print(type(err))
            # Create the file if it does not exist
            commit = repo.create_file(
                path=filename,
                message=f"Add initial {filename}",
                content=tf_str,
            )

        return commit["commit"].sha


github_storage = GithubStorage()


class MagicCastleORM(db.Model):
    __tablename__ = "magiccastle"
    id = db.Column(db.Integer, primary_key=True)
    hostname = db.Column(db.String(256), unique=True, nullable=False)

    tfcloud_workspace = db.Column(db.String(256))
    tfcloud_last_run = db.Column(db.String(256))

    status = db.Column(db.Enum(ClusterStatusCode), default=ClusterStatusCode.NOT_FOUND)
    plan_type = db.Column(db.Enum(PlanType), default=PlanType.NONE)
    created = db.Column(db.DateTime(), default=func.now())
    expiration_date = db.Column(db.String(32))
    config = db.Column(db.PickleType())
    applied_config = db.Column(db.PickleType())
    tf_state = db.Column(db.PickleType())
    plan = db.Column(db.PickleType())
    project_id = db.Column(db.Integer, db.ForeignKey("project.id"))
    project = db.relationship(
        "Project",
        back_populates="magic_castles",
        uselist=False,
        cascade_backrefs=False,
    )


class TerraformCloud:
    BASE_URL = "https://app.terraform.io/api/v2"

    def __init__(self) -> None:
        config = get_config()

        self.organisation_name = config["tfcloud_organization"]
        self.oauth_token_id = config["tfcloud_oauth_vcs_token_id"]

        self.headers = {
            "Authorization": f"Bearer {config['tfcloud_api_token']}",
            "Content-Type": "application/vnd.api+json",
        }

        self.workspace_url = (
            f"{self.BASE_URL}/organizations/{self.organisation_name}/workspaces"
        )

    def _request(self, method, url, **kwargs):
        return requests.request(method, url, headers=self.headers, **kwargs)

    def create_workspace(self, workspace_name, repo_full_name):
        workspace_payload = {
            "data": {
                "type": "workspaces",
                "attributes": {
                    "name": workspace_name,
                    "execution-mode": "remote",
                    "auto-apply": "true",
                    "auto-apply-run-trigger": "true",
                    "file-triggers-enabled": "false",
                    "queue-all-runs": "true",  # Must be set to true to trigger the first run automacticlly
                    "vcs-repo": {
                        "identifier": repo_full_name,
                        "oauth-token-id": self.oauth_token_id,
                        "branch": "main",
                        "default-branch": True,
                    },
                },
            }
        }

        response = self._request("POST", self.workspace_url, json=workspace_payload)

        try:
            workspace_id = response.json()["data"]["id"]
        except Exception:
            raise TerraformCloudException(
                "Could not create workspace",
                additional_details=f"{workspace_name=}, error: {response.text}",
            )
        return workspace_id

    def get_last_run_status(self, workspace_id):
        url = f"{self.BASE_URL}/workspaces/{workspace_id}/runs"
        params = {
            "page[size]": 1,  # Limit to the most recent run
        }
        res = self._request("GET", url, params=params)
        if res.status_code == 200:
            status = res.json()["data"][0]["attributes"]["status"]
            run_id = res.json()["data"][0]["id"]
            return run_id, TFCloudStatusCode(status)
            # Get planID: res.json()["data"][0]["relationships"]["plan"]["id"]
            # Get applyID: res.json()["data"][0]["relationships"]["apply"]["id"]
        else:
            raise TerraformCloudException(
                "Could not trigger run",
                additional_details=f"{workspace_id=}, error: {res.text}",
            )


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
                plan_type=PlanType.NONE,
                config={},
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
    def tfcloud_last_run(self):
        return self.orm.tfcloud_last_run

    @property
    def path(self):
        return path.join(CLUSTERS_PATH, self.hostname)

    @property
    def main_file(self):
        return path.join(self.path, MAIN_TERRAFORM_FILENAME)

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
        if self.orm.status == ClusterStatusCode.PROVISIONING_RUNNING:
            now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            if ProvisioningManager.check_online(self.hostname):
                self.status = ClusterStatusCode.PROVISIONING_SUCCESS
            elif MAX_PROVISIONING_TIME < (now - self.orm.created).total_seconds():
                self.status = ClusterStatusCode.PROVISIONING_ERROR

        return self.orm.status

    @status.setter
    def status(self, status: ClusterStatusCode):
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

    def rotate_terraform_logs(self, *, apply: bool):
        raise NotImplementedError
        """
        Rotates filenames for logs generated by running `terraform plan` or `terraform apply`.

        For instance, it will rename an existing file named terraform_plan.log to terraform_plan.log.1.
        Any log file already ending with a number will have its number incremented by one
        (e.g.  terraform_plan.log.1 would be renamed to terraform_plan.log.2).

        :param apply: True to rotate logs of `terraform apply`, False to rotate logs of `terraform plan`.
        """
        if apply:
            base_file_name = TERRAFORM_APPLY_LOG_FILENAME
        else:
            base_file_name = TERRAFORM_PLAN_LOG_FILENAME

        logs_path = path.join(CLUSTERS_PATH, self.hostname)
        old_file_names = []
        with scandir(logs_path) as it:
            for entry in it:
                if entry.is_file() and entry.name.startswith(base_file_name):
                    old_file_names.append(entry.name)

        # Sort alphabetically to always rename the log file with the highest index first
        old_file_names.sort(reverse=True)
        for old_file_name in old_file_names:
            if old_file_name == base_file_name:
                # terraform_apply.log becomes terraform_apply.log.1
                new_file_index = 1
            else:
                # terraform_apply.log.1 becomes terraform_apply.log.2 and so on
                new_file_index = int(old_file_name.split(".")[-1]) + 1
            new_file_name = f"{base_file_name}.{new_file_index}"
            rename(
                path.join(self.path, old_file_name),
                path.join(self.path, new_file_name),
            )

    @property
    def plan_type(self) -> PlanType:
        return self.orm.plan_type

    @plan_type.setter
    def plan_type(self, plan_type: PlanType):
        self.orm.plan_type = plan_type

    @property
    def plan(self) -> dict:
        return self.orm.plan

    @plan.setter
    def plan(self, plan: dict):
        self.orm.plan = plan

    def get_progress(self):
        if self.plan is None:
            return None

        try:
            with open(path.join(self.path, TERRAFORM_APPLY_LOG_FILENAME), "r") as file:
                terraform_output = file.read()
        except FileNotFoundError:
            # terraform apply was not launched yet, therefore the log file does not exist
            terraform_output = ""
        return TerraformPlanParser.get_done_changes(self.plan, terraform_output)

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
        self.plan_type = PlanType.BUILD
        db.session.add(self.orm)
        try:
            db.session.commit()
        except IntegrityError:
            raise ClusterExistsException

        github_repo_fullname = github_storage.create_repo(self.hostname)
        # Write the main terraform file to storage backend
        try:
            var_tf = self.config.get_var_tf()
            github_commit = github_storage.write(var_tf, self.hostname)
        except Exception as error:
            self.delete()
            raise PlanException(
                "Could not write variables.tf on the storage backend.",
                additional_details=f"hostname: {self.hostname}, error: {error}",
            )
        logging.info(
            f"{self.hostname}: New commit <{github_commit}> on repo <{github_repo_fullname}>"
        )

        workspace_name = github_repo_fullname.split("/")[-1]

        tf = TerraformCloud()
        workspace_id = tf.create_workspace(workspace_name, github_repo_fullname)

        logging.info(
            f"{self.hostname}: TerraformCloud workspace=<{workspace_id}> created"
        )

        self.orm.tfcloud_workspace = workspace_id

        self.status = ClusterStatusCode.CREATED
        run_id = None
        time.sleep(
            10
        )  # TODO: This is temporary, we will do a thread/coroutine that check the state periodiclly on TFCloud
        while self.status in [
            ClusterStatusCode.CREATED,
            ClusterStatusCode.PLAN_RUNNING,
        ]:
            run_id, tf_status = tf.get_last_run_status(workspace_id)
            self.status = ClusterStatusCode.from_tfcloudstatus(tf_status)
            time.sleep(5)

        self.orm.tfcloud_last_run = run_id
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
        if config_changed:
            try:
                var_tf = self.config.get_var_tf()
                github_storage.write(var_tf, self.hostname)
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
        # TODO: Implement destruction
        raise NotImplementedError

        if self.is_busy:
            raise BusyClusterException

        self.plan_type = PlanType.DESTROY
        if self.tf_state is not None:
            self.remove_existing_plan()
            self.rotate_terraform_logs(apply=False)
            self.create_plan()
        else:
            self.delete()

    def create_plan(self):
        logging.debug(f"Call <{self.__class__.__name__}:create_plan>")
        raise NotImplementedError

    def apply(self):
        logging.debug(f"Call <{self.__class__.__name__}:apply>")
        raise NotImplementedError

    def delete(self):
        # TODO: Remove github repo + workspace
        # Removes the content of the cluster's folder, even if not empty
        rmtree(self.path, ignore_errors=True)
        db.session.delete(self.orm)
        db.session.commit()

    def remove_existing_plan(self):
        try:
            # Remove existing plan, if it exists
            remove(path.join(self.path, TERRAFORM_PLAN_BINARY_FILENAME))
        except FileNotFoundError:
            # Must be a new cluster, without existing plans
            pass
