from typing import Optional, List
from dataclasses import dataclass

from mchub.models.magic_castle.terraform_cloud_status import TFCloudStatusCode

from ..configuration import get_config
import requests

from ..exceptions.server_exception import (
    TerraformCloudException,
)


@dataclass
class TerraformCloudVariable:
    name: str
    value: str
    sensitive: bool

    def to_dict(self):
        return {
            "type": "vars",
            "attributes": {
                "key": self.name,
                "value": self.value,
                "description": "",
                "category": "env",
                "hcl": False,
                "sensitive": self.sensitive,
            },
        }


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

        self.runs_url = f"{self.BASE_URL}/runs"

    def _request(self, method, url, **kwargs):
        return requests.request(method, url, headers=self.headers, **kwargs)

    def destroy_run(self, workspace_id):
        destroy_payload = {
            "data": {
                "attributes": {"message": "Apply destroy", "is-destroy": True},
                "type": "runs",
                "relationships": {
                    "workspace": {
                        "data": {"type": "workspaces", "id": f"{workspace_id}"}
                    },
                },
            }
        }

        response = self._request("POST", self.runs_url, json=destroy_payload)

        try:
            run_id = response.json()["data"]["id"]
        except Exception:
            raise TerraformCloudException(
                "Could not destroy workspace",
                additional_details=f"{workspace_id=}, error: {response.text}",
            )
        return run_id

    def create_project(self, project_name):
        url = f"{self.BASE_URL}/organizations/{self.organisation_name}/projects"
        payload = {
            "data": {
                "attributes": {
                    "name": project_name,
                    "description": f"MCHub project: {project_name}",
                    "default-execution-mode": "default",
                    # "setting-overwrites": null,
                },
                "type": "projects",
                "relationships": {
                    "organization": {
                        "data": {"id": self.organisation_name, "type": "organizations"}
                    }
                },
            }
        }
        response = self._request("POST", url, json=payload)

        try:
            project_id = response.json()["data"]["id"]
        except Exception:
            raise TerraformCloudException(
                "Could not create workspace",
                additional_details=f"{project_name=}, error: {response.text}",
            )
        return project_id

    def create_workspace(self, workspace_name, repo_full_name, project_id):
        workspace_payload = {
            "data": {
                "type": "workspaces",
                "attributes": {
                    "name": workspace_name,
                    "execution-mode": "remote",
                    "auto-apply": "true",
                    "auto-apply-run-trigger": "true",
                    "file-triggers-enabled": "false",
                    "queue-all-runs": "true",
                    "vcs-repo": {
                        "tags-regex": r"^apply-[a-f0-9]+$",
                        "identifier": repo_full_name,
                        "oauth-token-id": self.oauth_token_id,
                        "branch": "main",
                        "default-branch": True,
                    },
                },
                "relationships": {
                    "project": {"data": {"type": "projects", "id": project_id}}
                },
            }
        }

        response = self._request("POST", self.workspace_url, json=workspace_payload)

        try:
            workspace_id = response.json()["data"]["id"]
        except Exception:
            # TODO No error in UI (show Not Found)
            raise TerraformCloudException(
                "Could not create workspace",
                additional_details=f"{workspace_name=}, error: {response.text}",
            )
        return workspace_id

    def set_variable_set(
        self, project_id, project_name, variables: List[TerraformCloudVariable]
    ):
        url = f"{self.BASE_URL}/organizations/{self.organisation_name}/varsets"
        payload = {
            "data": {
                "type": "varsets",
                "attributes": {
                    "name": f"{project_name}",
                    "description": "variable set used for project={project_name}",
                    "global": False,
                    "priority": False,
                },
                "relationships": {
                    "organization": {
                        "data": {"type": "organizations", "id": self.organisation_name}
                    },
                    "parent": {"data": {"id": project_id, "type": "projects"}},
                    "projects": {"data": [{"id": project_id, "type": "projects"}]},
                    "vars": {"data": [var.to_dict() for var in variables]},
                },
            }
        }

        res = self._request("POST", url, json=payload)
        if res.status_code != 201:
            raise TerraformCloudException(
                "Could not set variable set",
                additional_details=f"{self.organisation_name=}, {project_name=} vars={[v.name for v in variables]}, error: {res.text}",
            )

    def get_lastest_run_status(self, workspace_id):
        url = f"{self.BASE_URL}/workspaces/{workspace_id}/runs"
        params = {
            "page[size]": 1,  # Limit to the most recent run
        }
        res = self._request("GET", url, params=params)
        if res.status_code == 200:
            try:
                status = res.json()["data"][0]["attributes"]["status"]
                is_detroy = res.json()["data"][0]["attributes"]["is-destroy"]
                run_id = res.json()["data"][0]["id"]
                return run_id, TFCloudStatusCode(status), is_detroy
            except IndexError:
                # No run found
                return None, None, None

        else:
            raise TerraformCloudException(
                "Could not find trigger run",
                additional_details=f"{workspace_id=}, error: {res.text}",
            )

    def get_run_apply_log(self, run_id) -> str:
        url = f"{self.BASE_URL}/runs/{run_id}/apply"
        res = self._request("GET", url)
        if res.status_code == 200:
            try:
                return res.json()["data"]["attributes"]["log-read-url"]

            except IndexError:
                raise TerraformCloudException(
                    "Could not find log url",
                    additional_details=f"{run_id=}, error: {res.text}",
                )

        else:
            raise TerraformCloudException(
                "Could not find apply run log",
                additional_details=f"{run_id=}, error: {res.text}",
            )

    def get_run_plan_log_json(self, run_id) -> Optional[dict]:
        url = f"{self.BASE_URL}/runs/{run_id}/plan"
        res = self._request("GET", url)
        if res.status_code == 200:
            try:
                is_finished = res.json()["data"]["attributes"]["status"] == "finished"
                plan_id = res.json()["data"]["id"]

                if is_finished:
                    log_url = f"{self.BASE_URL}/plans/{plan_id}/json-output"
                    return self._request("GET", log_url).json()
                else:
                    return None
            except IndexError:
                raise TerraformCloudException(
                    "Could not find plan id",
                    additional_details=f"{run_id=}, error: {res.text}",
                )

        else:
            raise TerraformCloudException(
                "Could not find apply log",
                additional_details=f"{run_id=}, error: {res.text}",
            )


_terraform_cloud_instance = None


def get_terraform_cloud() -> TerraformCloud:
    global _terraform_cloud_instance
    if _terraform_cloud_instance is None:
        _terraform_cloud_instance = TerraformCloud()
    return _terraform_cloud_instance
