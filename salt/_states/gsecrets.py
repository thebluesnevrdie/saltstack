# -*- coding: utf-8 -*-
'''
Module for managing secrets in Google Secrets Manager

Example:

.. code-block:: yaml


    This secret should not exist:
      gsecrets.absent:
        - name: cody-test-2
        - project_id: gcloud-test

    Manage this secret:
      gsecrets.managed:
        - name: cody-test-1
        - project_id: gcloud-test
        - secret: 8fa6o82b-f3ac-42f7-9f84-b9jjjjj14


'''

import json, logging

from salt.exceptions import CommandExecutionError
from salt.utils.stringutils import dequote

log = logging.getLogger(__name__)


def __virtual__():
    return 'gsecrets'


def absent(name, project_id):
    ret = {
        'name': name,
        'changes': {},
        'result': False,
        'comment': ''
    }

    our_secret = __salt__['gsecrets.exists'](project_id, name)
    if our_secret:
        __salt__['gsecrets.delete'](project_id, name)
        ret['result'] = True
        ret['comment'] = ''
        ret['changes'] = {
            "old": "present",
            "new": None
        }
    else:
        ret['result'] = True
        ret['comment'] = 'Secret not found.'
    return ret


def managed(name, secret, project_id, update_secret=False):
    ret = {
        'name': name,
        'changes': {},
        'result': False,
        'comment': ''
    }

    our_secret = __salt__['gsecrets.exists'](project_id, name)
    if our_secret:
        if update_secret:
            secret_data = __utils__['gsecrets.get_secret_data'](project_id, name)

            # We need a dump (temp) variable because gsecrets.put will json.dumps as well
            try:
                secret_dump = json.dumps(secret)
            except json.decoder.JSONDecodeError:
                log.warning("secret is not JSON")
                secret_dump = secret

            if secret_data != dequote(secret_dump):
                if __salt__['gsecrets.put'](project_id, name, secret):
                    ret['changes'] = {
                        "old": "present with different value",
                        "new": "present with updated value"
                    }
                    ret['result'] = True
                else:
                    ret['comment'] = "Error updating secret value"
            else:
                ret['result'] = True # update_secret is True, but secret values do not differ
        else:
            ret['result'] = True # update_secret is False, and secret exists
    else:
        ret['comment'] = 'Secret not found.'
        if __salt__['gsecrets.put'](project_id, name, secret):
            ret['changes'] = {
                    "old": "",
                    "new": "created"
                }
            ret['result'] = True
        else:
            ret['comment'] = "Error creating secret"
    return ret
