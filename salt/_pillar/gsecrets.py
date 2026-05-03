"""
Use Google Secrets Manager data as a Pillar source

Example configuration:

.. code-block:: yaml

    ext_pillar:
      - gsecrets:
          namespace: secrets
          project_id: google-test
          top:
            node:
              - database_pw


"""

import json, logging

__virtualname__ = "gsecrets"

# Set up logging
log = logging.getLogger(__name__)


def __virtual__():
    return __virtualname__


def _deserialize(secret_data):
    try:
        secret_data = json.loads(secret_data)
    except json.decoder.JSONDecodeError:
        log.debug("secret data is not JSON")
    return secret_data


def ext_pillar(minion_id, pillar, projects, namespace=None):
    """
    Check for secrets and return all data
    """

    pillar_data = {}

    for project_id in projects.keys():
        temp_data = {}
        # populate temp_data with all secret data
        for secret_name in __utils__["gsecrets.list_secrets"](project_id):
            raw_data = __utils__["gsecrets.get_secret_data"](project_id, secret_name)
            if raw_data[0]:
                temp_data[secret_name] = raw_data[1]

        if not projects[project_id]["top"]:
            log.warning("gsecrets ext_pillar: top key must be specified!")
        else:
            for entry in projects[project_id]["top"].keys():
                if minion_id.startswith(entry):
                    for sec_entry in projects[project_id]["top"][entry]:
                        for secret_id, secret_data in temp_data.items():
                            if secret_id.startswith(sec_entry):
                                pillar_data[secret_id] = _deserialize(secret_data)

    # if namespace is specified and our pillar_data is not empty
    if namespace and len(pillar_data):
        return {namespace: pillar_data}
    else:
        return pillar_data
