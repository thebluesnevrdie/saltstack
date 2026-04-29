# -*- coding: utf-8 -*-
"""
Library for interacting with Google Secrets Manager

:depends:  - google-cloud-secret-manager

"""

from salt.utils.stringutils import dequote

import logging

# Set up logging
log = logging.getLogger(__name__)

try:
    from google.cloud import secretmanager

    HAS_LIBS = True
except ImportError:
    HAS_LIBS = False


def __virtual__():
    """
    Only return if google-cloud-secret-manager is installed
    """
    if HAS_LIBS:
        return True
    else:
        return (
            False,
            "Missing dependency: install google-cloud-secret-manager module",
        )


def list_secrets(project_id):
    secrets_list = []
    client = secretmanager.SecretManagerServiceClient()
    request = secretmanager.ListSecretsRequest(
        parent=f"projects/{project_id}",
    )
    page_result = client.list_secrets(request=request)
    for response in page_result:
        name = response.name.split("/")[-1]
        log.debug(f"Found Secret: {name}")
        secrets_list.append(name)
    return secrets_list


def list_secret_versions(project_id, secret_id):
    version_state = {0: "Unspecified", 1: "Enabled", 2: "Disabled", 3: "Destroyed"}
    versions = {}
    client = secretmanager.SecretManagerServiceClient()
    request = secretmanager.ListSecretVersionsRequest(
        parent=f"projects/{project_id}/secrets/{secret_id}",
    )
    page_result = client.list_secret_versions(request=request)
    for response in page_result:
        num = int(response.name.split("/")[-1])
        versions[num] = {"state": version_state[response.state]}
    return versions


def get_secret_data(project_id, secret_id, version_id):
    if version_id != "latest":
        try:
            version_id = int(version_id)
        except:
            return [False, f"Version {version_id} must be 'latest' or a number"]
    versions = list_secret_versions(project_id, secret_id)
    log.debug(f"versions = {versions}")
    if version_id != "latest":
        if version_id in versions.keys():
            state = versions[version_id]["state"]
            if state != "Enabled":
                return [
                    False,
                    f"Version {version_id} for secret {secret_id} is in {state} state",
                ]
        else:
            return [False, f"No such version {version_id} for secret {secret_id}"]
    else:
        enabled = []
        for version, value in versions.items():
            if value["state"] == "Enabled":
                enabled.append(version)
        if enabled:
            version_id = max(enabled)
        else:
            return [False, f"Secret {secret_id} has no enabled versions"]

    client = secretmanager.SecretManagerServiceClient()
    secret_detail = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": secret_detail})
    data = response.payload.data.decode("UTF-8")
    return [True, data]


def create_secret(project_id, secret_id):
    client = secretmanager.SecretManagerServiceClient()
    project_detail = f"projects/{project_id}"

    try:
        response = client.create_secret(
            request={
                "parent": project_detail,
                "secret_id": secret_id,
                "secret": {"replication": {"automatic": {}}},
            }
        )
        log.debug(response)
        return True
    except Exception as e:
        log.debug(f"ERROR - gsecrets.create_secret exception: {e}")
        return False


def create_secret_version(project_id, secret_id, data):
    client = secretmanager.SecretManagerServiceClient()
    parent = client.secret_path(project_id, secret_id)

    tmp_data = dequote(data)
    encoded_data = tmp_data.encode("UTF-8")

    try:
        response = client.add_secret_version(
            request={"parent": parent, "payload": {"data": encoded_data}}
        )
        log.debug(response)
        return True
    except Exception as e:
        log.debug(f"ERROR - gsecrets.create_secret_version exception: {e}")
        return False


def delete_secret(project_id, secret_id):
    client = secretmanager.SecretManagerServiceClient()
    secret_detail = f"projects/{project_id}/secrets/{secret_id}"
    try:
        response = client.delete_secret(request={"name": secret_detail})
        log.debug(response)
        return True
    except Exception as e:
        log.debug(f"ERROR - gsecrets.delete_secret exception: {e}")
        return False
