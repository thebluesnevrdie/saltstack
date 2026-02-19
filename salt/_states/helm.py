# -*- coding: utf-8 -*-
'''
Updated version of the Helm Module
                                                                                                                                                                                      
:codeauthor:    Cody Crawford (https://github.com/thebluesnevrdie/saltstack)
:maturity:      new
:platform:      all

'''

import logging
import yaml

from salt.exceptions import CommandExecutionError
from salt.utils.dictdiffer import deep_diff
import salt.utils.dictupdate as dictupdate

log = logging.getLogger(__name__)


def repo_managed(
    name,
    present=None,
    absent=None,
    prune=False,
    repo_update=False,
    namespace=None,
    flags=None,
    kvflags=None,
):
    """
    Make sure the repository is updated.

    name
        (string) Not used.

    present
        (list) List of repository to be present. It's a list of dict: [{'name': 'local_name', 'url': 'repository_url'}]

    absent
        (list) List of local name repository to be absent.

    prune
        (boolean - default: False) If True, all repository already present but not in the present list would be removed.

    repo_update
        (boolean - default: False) If True, the Helm repository is updated after a repository add or remove.

    namespace
        (string) The namespace scope for this request.

    flags
        (list) Flags in argument of the command without values. ex: ['help', '--help']

    kvflags
        (dict) Flags in argument of the command with values. ex: {'v': 2, '--v': 4}

    Example:

    .. code-block:: yaml

        helm_repository_is_managed:
          helm.repo_managed:
            - present:
              - name: local_name_1
                url: repository_url
            - absent:
              - local_name_2

    """
    ret = {
        "name": name,
        "changes": {},
        "result": True,
        "comment": "Helm repo is managed.",
    }

    if "helm.repo_manage" not in __salt__:
        ret["result"] = False
        ret["comment"] = "'helm.repo_manage' modules not available on this minion."
    elif "helm.repo_update" not in __salt__:
        ret["result"] = False
        ret["comment"] = "'helm.repo_update' modules not available on this minion."
    elif __opts__.get("test", False):
        ret["result"] = None
        ret["comment"] = "Helm repo would have been managed."
    else:
        try:
            result = __salt__["helm.repo_manage"](
                present=present,
                absent=absent,
                prune=prune,
                namespace=namespace,
                flags=flags,
                kvflags=kvflags,
            )

            if result["failed"]:
                ret["comment"] = "Failed to add or remove some repositories."
                ret["changes"] = result
                ret["result"] = False

            elif result["added"] or result["removed"]:
                if repo_update:
                    result_repo_update = __salt__["helm.repo_update"](
                        namespace=namespace, flags=flags, kvflags=kvflags
                    )
                    result.update({"repo_update": result_repo_update})

                ret["comment"] = "Repositories were added or removed."
                ret["changes"] = result

        except CommandExecutionError as err:
            ret["result"] = False
            ret["comment"] = "Failed to add some repositories: {}.".format(err)

    return ret


def repo_updated(name, namespace=None, flags=None, kvflags=None):
    """
    Make sure the repository is updated.
    To execute after a repository changes.

    name
        (string) Not used.

    namespace
        (string) The namespace scope for this request.

    flags
        (list) Flags in argument of the command without values. ex: ['help', '--help']

    kvflags
        (dict) Flags in argument of the command with values. ex: {'v': 2, '--v': 4}

    Example:

    .. code-block:: yaml

        helm_repository_is_updated:
          helm.repo_updated

    """
    ret = {
        "name": name,
        "changes": {},
        "result": True,
        "comment": "Helm repo is updated.",
    }

    if "helm.repo_update" not in __salt__:
        ret["result"] = False
        ret["comment"] = "'helm.repo_update' modules not available on this minion."
    elif __opts__.get("test", False):
        ret["result"] = None
        ret["comment"] = "Helm repo would have been updated."
    else:
        try:
            result = __salt__["helm.repo_update"](
                namespace=namespace, flags=flags, kvflags=kvflags
            )
            if not (isinstance(result, bool) and result):
                ret["result"] = False
                ret["changes"] = result
                ret["comment"] = "Failed to sync some repositories."

        except CommandExecutionError as err:
            ret["result"] = False
            ret["comment"] = "Failed to update some repositories: {}.".format(err)

    return ret


def release_managed(
    name,
    chart,
    values=None,
    version=None,
    namespace=None,
    create_namespace=True,
):
    """
    Make sure the release name is present.

    name
        (string) The release name to install.

    chart
        (string) The chart to install.

    values
        (dict) Values to be passed to Helm (as in a values.yaml file)

    version
        (string) The exact chart version to install. If this is not specified, the latest version is installed.

    namespace
        (string) The namespace scope for this request.

    Example:

    .. code-block:: yaml

        helm_release_is_present:
          helm.release_present:
            - name: release_name
            - chart: repo/chart

        # In dry-run mode.
        helm_release_is_present_dry-run:
          helm.release_present:
            - name: release_name
            - chart: repo/chart
            - flags:
              - dry-run

        # With values.yaml file.
        helm_release_is_present_values:
          helm.release_present:
            - name: release_name
            - chart: repo/chart
            - kvflags:
                values: /path/to/values.yaml

    """
    ret = {
        "name": name,
        "changes": {},
        "result": True,
        "comment": "Helm release {} is present with correct values".format(name),
    }

    if "helm.status" not in __salt__:
        ret["result"] = False
        ret["comment"] = "'helm.status' modules not available on this minion."
    elif "helm.upgrade" not in __salt__:
        ret["result"] = False
        ret["comment"] = "'helm.upgrade' modules not available on this minion."
    elif __opts__.get("test", False):
        ret["result"] = None
        ret["comment"] = "Helm release would have been installed or updated."
    else:
        release_old_status = __salt__["helm.status"](
            release=name, namespace=namespace, sections=["config"]
        )
        if isinstance(release_old_status, dict):
            if values and (values != release_old_status["config"]):
                dictupdate.update(
                    ret["changes"], deep_diff(release_old_status["config"], values)
                )
        else:
            if values:
                ret["changes"] = {"old": "", "new": values}
            else:
                ret["changes"] = {
                    "old": "",
                    "new": release_cur_status["info"]["status"],
                }

        if ret["changes"]:
            release_upgrade = __salt__["helm.upgrade"](
                release=name,
                chart=chart,
                values=values,
                version=version,
                namespace=namespace,
                create_namespace=create_namespace,
            )
            if isinstance(release_upgrade, bool) and release_upgrade:
                release_cur_status = __salt__["helm.status"](
                    release=name, namespace=namespace, sections=["info"]
                )
                if isinstance(release_cur_status, dict):
                    ret["comment"] = (
                        "Status: "
                        + release_cur_status["info"]["status"]
                        + " - "
                        + release_cur_status["info"]["description"]
                    )
                else:
                    ret["result"] = False
                    ret["comment"] = release_cur_status
            else:
                ret["result"] = False
                ret["comment"] = release_upgrade

    return ret


def release_absent(name, namespace=None, flags=None, kvflags=None):
    """
    Make sure the release name is absent.

    name
        (string) The release name to uninstall.

    namespace
        (string) The namespace scope for this request.

    flags
        (list) Flags in argument of the command without values. ex: ['help', '--help']

    kvflags
        (dict) Flags in argument of the command with values. ex: {'v': 2, '--v': 4}

    Example:

    .. code-block:: yaml

        helm_release_is_absent:
          helm.release_absent:
            - name: release_name

        # In dry-run mode.
        helm_release_is_absent_dry-run:
          helm.release_absent:
            - name: release_name
            - flags:
              - dry-run

    """
    ret = {
        "name": name,
        "changes": {},
        "result": True,
        "comment": "Helm release {} is absent.".format(name),
    }

    if "helm.uninstall" not in __salt__:
        ret["result"] = False
        ret["comment"] = "'helm.uninstall' modules not available on this minion."
    elif "helm.status" not in __salt__:
        ret["result"] = False
        ret["comment"] = "'helm.status' modules not available on this minion."
    elif __opts__.get("test", False):
        ret["result"] = None
        ret["comment"] = "Helm release would have been uninstalled."
    else:
        release_status = __salt__["helm.status"](release=name, namespace=namespace)
        if isinstance(release_status, dict):
            release_uninstall = __salt__["helm.uninstall"](
                release=name, namespace=namespace, flags=flags, kvflags=kvflags
            )
            if isinstance(release_uninstall, bool) and release_uninstall:
                ret["changes"] = {"absent": name}
            else:
                ret["result"] = False
                ret["comment"] = release_uninstall

    return ret
