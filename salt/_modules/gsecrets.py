# -*- coding: utf-8 -*-
"""
Manage Google Secrets Manager data

Examples:

.. code-block:: yaml

    salt '*' gsecrets.exists gcloud-test my-secret-key

    # or

    salt '*' gsecrets.put gcloud-test my-secret-key 'super_secret_data'

"""

import json, logging

__virtualname__ = "gsecrets"

# Set up logging
log = logging.getLogger(__name__)


def __virtual__():
    return __virtualname__

def exists(project_id, secret_id):
    """
    Check for secret existence
    """

    for secret_name in __utils__["gsecrets.list_secrets"](project_id):
        if secret_name == secret_id:
            return True

    return False


def list(project_id):
    return __utils__["gsecrets.list_secrets"](project_id)


def versions(project_id, secret_id):
    return __utils__["gsecrets.list_secret_versions"](project_id, secret_id)


def get(project_id, secret_id, version_id="latest"):
    """
    Get secret value
    """

    if exists(project_id, secret_id):
        our_secret = __utils__['gsecrets.get_secret_data'](project_id, secret_id, version_id)
        if our_secret[0]:
            return our_secret[1]
        else:
            return [
                False,
                f"Error: {our_secret[1]}"
            ]
    else:
        return (f"Secret {secret_id} does not exist in project {project_id}")


def put(project_id, secret_id, secret_data):
    """
    Set secret value
    """

    try:
        secret_data = json.dumps(secret_data)
    except json.decoder.JSONDecodeError:
        log.debug("secret data is not JSON")

    if not exists(project_id, secret_id):
        __utils__["gsecrets.create_secret"](project_id, secret_id)

    return __utils__["gsecrets.create_secret_version"](
        project_id, secret_id, secret_data
    )


def delete(project_id, secret_id):
    """
    Delete secret value
    """

    return __utils__["gsecrets.delete_secret"](project_id, secret_id)
