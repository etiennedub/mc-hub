from enum import Enum
from .terraform_cloud_status import TFCloudStatusCode


class ClusterStatusCode(str, Enum):
    CREATED = "created"
    PLAN_RUNNING = "plan_running"
    PLAN_ERROR = "plan_error"

    # TODO: Remove build
    BUILD_RUNNING = "build_running"
    BUILD_ERROR = "build_error"

    PROVISIONING_RUNNING = "provisioning_running"
    PROVISIONING_SUCCESS = "provisioning_success"
    PROVISIONING_ERROR = "provisioning_error"
    DESTROY_RUNNING = "destroy_running"
    DESTROY_ERROR = "destroy_error"
    NOT_FOUND = "not_found"

    @staticmethod
    def from_tfcloudstatus(tf_status: TFCloudStatusCode):
        """
        Those status are not supported with TFCloud
        ClusterStatusCode.BUILD_ERROR
        ClusterStatusCode.BUILD_RUNNING
        ClusterStatusCode.PROVISIONING_ERROR
        ClusterStatusCode.DESTROY_RUNNING
        ClusterStatusCode.DESTROY_ERROR
        """
        match tf_status:
            case (
                TFCloudStatusCode.PENDING
                | TFCloudStatusCode.PLAN_QUEUED
                | TFCloudStatusCode.FETCHING
                | TFCloudStatusCode.FETCHING_COMPLETED
                | TFCloudStatusCode.PRE_PLAN_COMPLETED
                | TFCloudStatusCode.QUEUING
            ):
                return ClusterStatusCode.CREATED
            case TFCloudStatusCode.PLANNING | TFCloudStatusCode.PRE_PLAN_RUNNING:
                return ClusterStatusCode.PLAN_RUNNING
            case (
                TFCloudStatusCode.DISCARDED
                | TFCloudStatusCode.ERRORED
                | TFCloudStatusCode.CANCELED
                | TFCloudStatusCode.FORCE_CANCELED
            ):
                # TODO: When an error occur, we can fetch the plan and apply to know the corresponding step.
                # For now, all errors are return as PLAN_ERROR
                return ClusterStatusCode.PLAN_ERROR
            case (
                TFCloudStatusCode.PLANNED_AND_FINISHED
                | TFCloudStatusCode.PLANNED_AND_SAVED
                | TFCloudStatusCode.APPLY_QUEUED
                | TFCloudStatusCode.APPLYING
                | TFCloudStatusCode.COST_ESTIMATING
                | TFCloudStatusCode.COST_ESTIMATED
                | TFCloudStatusCode.POLICY_CHECKING
                | TFCloudStatusCode.POLICY_OVERRIDE
                | TFCloudStatusCode.POLICY_SOFT_FAILED
                | TFCloudStatusCode.POLICY_CHECKED
                | TFCloudStatusCode.POST_PLAN_RUNNING
                | TFCloudStatusCode.POST_PLAN_COMPLETED
            ):
                return ClusterStatusCode.PROVISIONING_RUNNING
            case TFCloudStatusCode.APPLIED:
                return ClusterStatusCode.PROVISIONING_SUCCESS

            case TFCloudStatusCode.CONFIRMED:
                raise NotImplementedError(f"TFCloud status is unsuported: {tf_status=}")

            case _:
                return ClusterStatusCode.NOT_FOUND