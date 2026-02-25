# -*- coding: utf-8 -*-
'''
Updated version of the Kubernetes Module

:codeauthor:    Cody Crawford (https://github.com/thebluesnevrdie/saltstack)

Manage kubernetes resources as salt states
==========================================

NOTE: This module requires the proper pillar values set. See
salt.modules.kubernetesmod for more information.

.. warning::

    Configuration options will change in 2019.2.0.

The kubernetes module is used to manage different kubernetes resources.


.. code-block:: yaml

    my-nginx:
      kubernetes.deployment_present:
        - namespace: default
          metadata:
            app: frontend
          spec:
            replicas: 1
            template:
              metadata:
                labels:
                  run: my-nginx
              spec:
                containers:
                - name: my-nginx
                  image: nginx
                  ports:
                  - containerPort: 80

    my-mariadb:
      kubernetes.deployment_absent:
        - namespace: default

    # kubernetes deployment as specified inside of
    # a file containing the definition of the the
    # deployment using the official kubernetes format
    redis-master-deployment:
      kubernetes.deployment_present:
        - name: redis-master
        - source: salt://k8s/redis-master-deployment.yml
      require:
        - pip: kubernetes-python-module

    # kubernetes service as specified inside of
    # a file containing the definition of the the
    # service using the official kubernetes format
    redis-master-service:
      kubernetes.service_present:
        - name: redis-master
        - source: salt://k8s/redis-master-service.yml
      require:
        - kubernetes.deployment_present: redis-master

    # kubernetes deployment as specified inside of
    # a file containing the definition of the the
    # deployment using the official kubernetes format
    # plus some jinja directives
     nginx-source-template:
      kubernetes.deployment_present:
        - source: salt://k8s/nginx.yml.jinja
        - template: jinja
      require:
        - pip: kubernetes-python-module


    # Kubernetes secret
    k8s-secret:
      kubernetes.secret_present:
        - name: top-secret
          data:
            key1: value1
            key2: value2
            key3: value3

.. versionadded:: 2017.7.0

'''

import copy
import datetime
import json
import logging

import salt.utils.files

log = logging.getLogger(__name__)


def __virtual__():
    """
    Only load if the kubernetes module is available in __salt__
    """
    if "kubernetes.ping" in __salt__:
        return True
    return (False, "kubernetes module could not be loaded")


def __serialize_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")


def __render_body(
    name,
    body,
    source,
    template,
    saltenv,
    context=None,
    defaults=None    
):
    """
    Create a Kubernetes Object body
    """

    log.debug(f"******* __render_body *******\ndefaults: {defaults}")
    if source and body:
        raise CommandExecutionError(
            "Only one of body or source parameter should be passed."
        )

    if source:
        body = __read_and_render_yaml_file(source, template, saltenv, context, defaults)

    # ensure we're not using an OrderedDict
    # body = json.loads(json.dumps(body, default=__serialize_datetime))

    if "metadata" in body:
        body["metadata"]["name"] = name
    else:
        body["metadata"] = { "name": name }

    body["metadata"].pop("apiVersion", None)
    body["metadata"].pop("kind", None)
    body["metadata"].pop("namespace", None)

    return body

def __read_and_render_yaml_file(source, template, saltenv, context=None, defaults=None):
    """
    Read a yaml file and, if needed, renders that using the specifieds
    templating. Returns the python objects defined inside of the file.
    """

    sfn = __salt__["cp.cache_file"](source, saltenv)
    if not sfn:
        raise CommandExecutionError("Source file '{}' not found".format(source))

    with salt.utils.files.fopen(sfn, "r") as src:
        contents = src.read()

        if template:
            data = __salt__["file.apply_template_on_contents"](
                contents,
                template=template,
                context=context,
                defaults=defaults,
                saltenv=__env__,
            )
            log.debug(f"data: {data}")
            if not isinstance(data, str):
                raise CommandExecutionError(
                    "Failed to render file path with error: {}".format(data["comment"])
                )
            # contents = data["data"].encode("utf-8")

        return salt.utils.yaml.safe_load(data)


def _error(ret, err_msg):
    """
    Helper function to propagate errors to
    the end user.
    """
    ret["result"] = False
    ret["comment"] = err_msg
    return ret


def _is_subset(subset, superset):
    if isinstance(subset, dict):
        changes = { "new": {}, "old": {} }
        for key, value in subset.items():
            try:
                our_cmp = _is_subset(value, superset[key])
                if our_cmp["new"]:
                    changes["new"][key] = our_cmp["new"]
                    changes["old"][key] = our_cmp["old"]
            except KeyError:
                    changes["new"][key] = value
                    changes["old"][key] = None
            except TypeError:
                    changes["new"][key] = value
                    changes["old"] = superset
    elif isinstance(subset, list):
        changes = { "new": [], "old": [] }
        for index, value in enumerate(subset):
            our_cmp = _is_subset(value, superset[index])
            if our_cmp["new"]:
                changes["new"].append(our_cmp["new"])
                changes["old"].append(our_cmp["old"])
    else:
        if isinstance(superset, dict):
            changes = { "new": subset, "old": superset }
        elif isinstance(superset, list):
            if subset not in superset:
                changes = { "new": subset, "old": superset }
        elif subset != superset:
            changes = { "new": subset, "old": superset }
        else:
            changes = { "new": {}, "old": {} }

    return changes
            

def _manage_object(
    kind,
    name,
    body={},
    context=None,
    defaults=None,    
    namespace="default",
    patch=False,
    source=None,
    template=None,
    **kwargs
):

    ret = {"name": name, "changes": {}, "result": False, "comment": ""}

    old_object = __salt__["kubernetes.show_"+kind](name, namespace, **kwargs)

    body = __render_body(
        name=name,
        body=body,
        context=context,
        defaults=defaults,
        source=source,
        template=template,
        saltenv=__env__
    )

    if patch:
        if old_object is None:
            ret["comment"] = kind + " does not exist - cannot patch"
            return ret
        else:
            subset = _is_subset(body, old_object)
            if subset["new"]:
                ret["comment"] = f"Patched " + kind
                res = __salt__["kubernetes.patch_"+kind](
                    name=name,
                    namespace=namespace,
                    body=body
                )
                ret["changes"] = subset
    else:
        if old_object is None:
            if __opts__["test"]:
                ret["result"] = None
                ret["comment"] = "The " + kind + " is going to be created"
                return ret
            res = __salt__["kubernetes.create_"+kind](
                name=name,
                body=body,
                namespace=namespace,
                **kwargs
            )
            ret["changes"] = {"old": {}, "new": body}
        else:
            if __opts__["test"]:
                ret["result"] = None
                return ret

            subset = _is_subset(body, old_object)
            if subset["new"]:
                res = __salt__["kubernetes.replace_"+kind](
                    name=name,
                    body=body,
                    namespace=namespace,
                    **kwargs
                )

                ret["changes"] = subset

    ret["result"] = True
    return ret


def deployment_absent(name, namespace="default", **kwargs):
    """
    Ensures that the named deployment is absent from the given namespace.

    name
        The name of the deployment

    namespace
        The name of the namespace
    """

    ret = {"name": name, "changes": {}, "result": False, "comment": ""}

    deployment = __salt__["kubernetes.show_deployment"](name, namespace, **kwargs)

    if deployment is None:
        ret["result"] = True if not __opts__["test"] else None
        ret["comment"] = "The deployment does not exist"
        return ret

    if __opts__["test"]:
        ret["comment"] = "The deployment is going to be deleted"
        ret["result"] = None
        return ret

    res = __salt__["kubernetes.delete_deployment"](name, namespace, **kwargs)
    if res["code"] == 200:
        ret["result"] = True
        ret["changes"] = {"kubernetes.deployment": {"new": "absent", "old": "present"}}
        ret["comment"] = res["message"]
    else:
        ret["comment"] = "Something went wrong, response: {}".format(res)

    return ret


def manage_deployment(
    name,
    body={},
    context=None,
    defaults=None,    
    namespace="default",
    patch=False,
    source=None,
    template=None,
    **kwargs
):
    """
    Ensures that the named deployment is present inside of the specified
    namespace with the given body.
    If the deployment does not exist it will be created.

    name
        The name of the deployment.

    body
        The body of the deployment object.

    namespace
        The namespace holding the deployment. The 'default' one is going to be
        used unless a different one is specified.

    patch
        Only ensure deployment contains specified values, and add to existing
        configuration if not.

    source
        A file containing the definition of the deployment (metadata and
        spec) in the official kubernetes format.

    template
        Template engine to be used to render the source file.
    """


    return _manage_object(
        kind="deployment",
        name=name,
        body=body,
        context=context,
        defaults=defaults,    
        namespace=namespace,
        patch=patch,
        source=source,
        template=template,
        **kwargs
    )

    
def manage_ingress(
    name,
    body={},
    context=None,
    defaults=None,    
    namespace="default",
    patch=False,
    source=None,
    template=None,
    **kwargs
):
    """
    Ensures that the named ingress is present inside of the specified
    namespace with the given body.
    If the ingress does not exist it will be created.

    name
        The name of the ingress.

    body
        The body of the ingress object.

    namespace
        The namespace holding the ingress. The 'default' one is going to be
        used unless a different one is specified.

    patch
        Only ensure ingress contains specified values, and add to existing
        configuration if not.

    source
        A file containing the definition of the ingress (metadata and
        spec) in the official kubernetes format.

    template
        Template engine to be used to render the source file.
    """


    return _manage_object(
        kind="ingress",
        name=name,
        body=body,
        context=context,
        defaults=defaults,    
        namespace=namespace,
        patch=patch,
        source=source,
        template=template,
        **kwargs
    )

    
def ingress_absent(name, namespace="default", **kwargs):
    """
    Ensures that the named ingress is absent from the given namespace.

    name
        The name of the ingress

    namespace
        The name of the namespace
    """

    ret = {"name": name, "changes": {}, "result": False, "comment": ""}

    ingress = __salt__["kubernetes.show_ingress"](name, namespace, **kwargs)

    if ingress is None:
        ret["result"] = True if not __opts__["test"] else None
        ret["comment"] = "The ingress does not exist"
        return ret

    if __opts__["test"]:
        ret["comment"] = "The ingress is going to be deleted"
        ret["result"] = None
        return ret

    res = __salt__["kubernetes.delete_ingress"](name, namespace, **kwargs)
    if isinstance(res, dict):
        ret["result"] = True
        ret["changes"] = {"kubernetes.ingress": {"new": "absent", "old": "present"}}
        ret["comment"] = res["status"]
    else:
        ret["comment"] = "Something went wrong, response: {}".format(res)

    return ret


def manage_service(
    name,
    body={},
    context=None,
    defaults=None,    
    namespace="default",
    patch=False,
    source=None,
    template=None,
    **kwargs
):
    """
    Ensures that the named service is present inside of the specified
    namespace with the given body.
    If the service does not exist it will be created.

    name
        The name of the service.

    body
        The body of the service object.

    namespace
        The namespace holding the service. The 'default' one is going to be
        used unless a different one is specified.

    patch
        Only ensure service contains specified values, and add to existing
        configuration if not.

    source
        A file containing the definition of the service (metadata and
        spec) in the official kubernetes format.

    template
        Template engine to be used to render the source file.
    """


    return _manage_object(
        kind="service",
        name=name,
        body=body,
        context=context,
        defaults=defaults,    
        namespace=namespace,
        patch=patch,
        source=source,
        template=template,
        **kwargs
    )

    
def service_absent(name, namespace="default", **kwargs):
    """
    Ensures that the named service is absent from the given namespace.

    name
        The name of the service

    namespace
        The name of the namespace
    """

    ret = {"name": name, "changes": {}, "result": False, "comment": ""}

    service = __salt__["kubernetes.show_service"](name, namespace, **kwargs)

    if service is None:
        ret["result"] = True if not __opts__["test"] else None
        ret["comment"] = "The service does not exist"
        return ret

    if __opts__["test"]:
        ret["comment"] = "The service is going to be deleted"
        ret["result"] = None
        return ret

    res = __salt__["kubernetes.delete_service"](name, namespace, **kwargs)
    log.debug(f"********************************res:\n\n{res}")
    if isinstance(res, dict):
        ret["result"] = True
        ret["changes"] = {"kubernetes.service": {"new": "absent", "old": "present"}}
        ret["comment"] = res["status"]
    else:
        ret["comment"] = "Something went wrong, response: {}".format(res)

    return ret


def namespace_absent(name, **kwargs):
    """
    Ensures that the named namespace is absent.

    name
        The name of the namespace
    """

    ret = {"name": name, "changes": {}, "result": False, "comment": ""}

    namespace = __salt__["kubernetes.show_namespace"](name, **kwargs)

    if namespace is None:
        ret["result"] = True if not __opts__["test"] else None
        ret["comment"] = "The namespace does not exist"
        return ret

    if __opts__["test"]:
        ret["comment"] = "The namespace is going to be deleted"
        ret["result"] = None
        return ret

    res = __salt__["kubernetes.delete_namespace"](name, **kwargs)
    if (
        res["code"] == 200
        or (isinstance(res["status"], str) and "Terminating" in res["status"])
        or (isinstance(res["status"], dict) and res["status"]["phase"] == "Terminating")
    ):
        ret["result"] = True
        ret["changes"] = {"kubernetes.namespace": {"new": "absent", "old": "present"}}
        if res["message"]:
            ret["comment"] = res["message"]
        else:
            ret["comment"] = "Terminating"
    else:
        ret["comment"] = "Something went wrong, response: {}".format(res)

    return ret


def namespace_present(name, **kwargs):
    """
    Ensures that the named namespace is present.

    name
        The name of the namespace.

    """
    ret = {"name": name, "changes": {}, "result": False, "comment": ""}

    namespace = __salt__["kubernetes.show_namespace"](name, **kwargs)

    if namespace is None:
        if __opts__["test"]:
            ret["result"] = None
            ret["comment"] = "The namespace is going to be created"
            return ret

        res = __salt__["kubernetes.create_namespace"](name, **kwargs)
        ret["result"] = True
        ret["changes"]["namespace"] = {"old": {}, "new": res}
    else:
        ret["result"] = True if not __opts__["test"] else None
        ret["comment"] = "The namespace already exists"

    return ret


def secret_absent(name, namespace="default", **kwargs):
    """
    Ensures that the named secret is absent from the given namespace.

    name
        The name of the secret

    namespace
        The name of the namespace
    """

    ret = {"name": name, "changes": {}, "result": False, "comment": ""}

    secret = __salt__["kubernetes.show_secret"](name, namespace, **kwargs)

    if secret is None:
        ret["result"] = True if not __opts__["test"] else None
        ret["comment"] = "The secret does not exist"
        return ret

    if __opts__["test"]:
        ret["comment"] = "The secret is going to be deleted"
        ret["result"] = None
        return ret

    __salt__["kubernetes.delete_secret"](name, namespace, **kwargs)

    # As for kubernetes 1.6.4 doesn't set a code when deleting a secret
    # The kubernetes module will raise an exception if the kubernetes
    # server will return an error
    ret["result"] = True
    ret["changes"] = {"kubernetes.secret": {"new": "absent", "old": "present"}}
    ret["comment"] = "Secret deleted"
    return ret


def secret_present(
    name, namespace="default", data=None, source=None, template=None, **kwargs
):
    """
    Ensures that the named secret is present inside of the specified namespace
    with the given data.
    If the secret exists it will be replaced.

    name
        The name of the secret.

    namespace
        The namespace holding the secret. The 'default' one is going to be
        used unless a different one is specified.

    data
        The dictionary holding the secrets.

    source
        A file containing the data of the secret in plain format.

    template
        Template engine to be used to render the source file.
    """
    ret = {"name": name, "changes": {}, "result": False, "comment": ""}

    if data and source:
        return _error(ret, "'source' cannot be used in combination with 'data'")

    secret = __salt__["kubernetes.show_secret"](name, namespace, **kwargs)

    if secret is None:
        if data is None:
            data = {}

        if __opts__["test"]:
            ret["result"] = None
            ret["comment"] = "The secret is going to be created"
            return ret
        res = __salt__["kubernetes.create_secret"](
            name=name,
            namespace=namespace,
            data=data,
            source=source,
            template=template,
            saltenv=__env__,
            **kwargs
        )
        ret["changes"]["{}.{}".format(namespace, name)] = {"old": {}, "new": res}
    else:
        if __opts__["test"]:
            ret["result"] = None
            ret["comment"] = "The secret is going to be replaced"
            return ret

        # TODO: improve checks  # pylint: disable=fixme
        log.info("Forcing the recreation of the service")
        ret["comment"] = "The secret is already present. Forcing recreation"
        res = __salt__["kubernetes.replace_secret"](
            name=name,
            namespace=namespace,
            data=data,
            source=source,
            template=template,
            saltenv=__env__,
            **kwargs
        )

    ret["changes"] = {
        # Omit values from the return. They are unencrypted
        # and can contain sensitive data.
        "data": list(res["data"])
    }
    ret["result"] = True

    return ret


def configmap_absent(name, namespace="default", **kwargs):
    """
    Ensures that the named configmap is absent from the given namespace.

    name
        The name of the configmap

    namespace
        The namespace holding the configmap. The 'default' one is going to be
        used unless a different one is specified.
    """

    ret = {"name": name, "changes": {}, "result": False, "comment": ""}

    configmap = __salt__["kubernetes.show_configmap"](name, namespace, **kwargs)

    if configmap is None:
        ret["result"] = True if not __opts__["test"] else None
        ret["comment"] = "The configmap does not exist"
        return ret

    if __opts__["test"]:
        ret["comment"] = "The configmap is going to be deleted"
        ret["result"] = None
        return ret

    __salt__["kubernetes.delete_configmap"](name, namespace, **kwargs)
    # As for kubernetes 1.6.4 doesn't set a code when deleting a configmap
    # The kubernetes module will raise an exception if the kubernetes
    # server will return an error
    ret["result"] = True
    ret["changes"] = {"kubernetes.configmap": {"new": "absent", "old": "present"}}
    ret["comment"] = "ConfigMap deleted"

    return ret


def manage_configmap(
    name,
    body={},
    context=None,
    defaults=None,    
    namespace="default",
    patch=False,
    source=None,
    template=None,
    **kwargs
):
    """
    Ensures that the named configmap is present inside of the specified
    namespace with the given body.
    If the configmap does not exist it will be created.

    name
        The name of the configmap.

    body
        The body of the configmap object.

    namespace
        The namespace holding the configmap. The 'default' one is going to be
        used unless a different one is specified.

    patch
        Only ensure configmap contains specified values, and add to existing
        configuration if not.

    source
        A file containing the definition of the configmap (metadata and
        spec) in the official kubernetes format.

    template
        Template engine to be used to render the source file.
    """


    return _manage_object(
        kind="configmap",
        name=name,
        body=body,
        context=context,
        defaults=defaults,    
        namespace=namespace,
        patch=patch,
        source=source,
        template=template,
        **kwargs
    )

    
def pod_absent(name, namespace="default", **kwargs):
    """
    Ensures that the named pod is absent from the given namespace.

    name
        The name of the pod

    namespace
        The name of the namespace
    """

    ret = {"name": name, "changes": {}, "result": False, "comment": ""}

    pod = __salt__["kubernetes.show_pod"](name, namespace, **kwargs)

    if pod is None:
        ret["result"] = True if not __opts__["test"] else None
        ret["comment"] = "The pod does not exist"
        return ret

    if __opts__["test"]:
        ret["comment"] = "The pod is going to be deleted"
        ret["result"] = None
        return ret

    res = __salt__["kubernetes.delete_pod"](name, namespace, **kwargs)
    if res["code"] == 200 or res["code"] is None:
        ret["result"] = True
        ret["changes"] = {"kubernetes.pod": {"new": "absent", "old": "present"}}
        if res["code"] is None:
            ret["comment"] = "In progress"
        else:
            ret["comment"] = res["message"]
    else:
        ret["comment"] = "Something went wrong, response: {}".format(res)

    return ret


def manage_pod(
    name,
    body={},
    context=None,
    defaults=None,    
    namespace="default",
    patch=False,
    source=None,
    template=None,
    **kwargs
):
    """
    Ensures that the named pod is present inside of the specified
    namespace with the given body.
    If the pod does not exist it will be created.

    name
        The name of the pod.

    body
        The body of the pod object.

    namespace
        The namespace holding the pod. The 'default' one is going to be
        used unless a different one is specified.

    patch
        Only ensure pod contains specified values, and add to existing
        configuration if not.

    source
        A file containing the definition of the pod (metadata and
        spec) in the official kubernetes format.

    template
        Template engine to be used to render the source file.
    """


    return _manage_object(
        kind="pod",
        name=name,
        body=body,
        context=context,
        defaults=defaults,    
        namespace=namespace,
        patch=patch,
        source=source,
        template=template,
        **kwargs
    )

    
def node_label_absent(name, node, **kwargs):
    """
    Ensures that the named label is absent from the node.

    name
        The name of the label

    node
        The name of the node
    """

    ret = {"name": name, "changes": {}, "result": False, "comment": ""}

    labels = __salt__["kubernetes.node_labels"](node, **kwargs)

    if name not in labels:
        ret["result"] = True if not __opts__["test"] else None
        ret["comment"] = "The label does not exist"
        return ret

    if __opts__["test"]:
        ret["comment"] = "The label is going to be deleted"
        ret["result"] = None
        return ret

    __salt__["kubernetes.node_remove_label"](node_name=node, label_name=name, **kwargs)

    ret["result"] = True
    ret["changes"] = {"kubernetes.node_label": {"new": "absent", "old": "present"}}
    ret["comment"] = "Label removed from node"

    return ret


def node_label_folder_absent(name, node, **kwargs):
    """
    Ensures the label folder doesn't exist on the specified node.

    name
        The name of label folder

    node
        The name of the node
    """

    ret = {"name": name, "changes": {}, "result": False, "comment": ""}
    labels = __salt__["kubernetes.node_labels"](node, **kwargs)

    folder = name.strip("/") + "/"
    labels_to_drop = []
    new_labels = []
    for label in labels:
        if label.startswith(folder):
            labels_to_drop.append(label)
        else:
            new_labels.append(label)

    if not labels_to_drop:
        ret["result"] = True if not __opts__["test"] else None
        ret["comment"] = "The label folder does not exist"
        return ret

    if __opts__["test"]:
        ret["comment"] = "The label folder is going to be deleted"
        ret["result"] = None
        return ret

    for label in labels_to_drop:
        __salt__["kubernetes.node_remove_label"](
            node_name=node, label_name=label, **kwargs
        )

    ret["result"] = True
    ret["changes"] = {
        "kubernetes.node_label_folder_absent": {"old": list(labels), "new": new_labels}
    }
    ret["comment"] = "Label folder removed from node"

    return ret


def node_label_present(name, node, value, **kwargs):
    """
    Ensures that the named label is set on the named node
    with the given value.
    If the label exists it will be replaced.

    name
        The name of the label.

    value
        Value of the label.

    node
        Node to change.
    """
    ret = {"name": name, "changes": {}, "result": False, "comment": ""}

    labels = __salt__["kubernetes.node_labels"](node, **kwargs)

    if name not in labels:
        if __opts__["test"]:
            ret["result"] = None
            ret["comment"] = "The label is going to be set"
            return ret
        __salt__["kubernetes.node_add_label"](
            label_name=name, label_value=value, node_name=node, **kwargs
        )
    elif labels[name] == value:
        ret["result"] = True
        ret["comment"] = "The label is already set and has the specified value"
        return ret
    else:
        if __opts__["test"]:
            ret["result"] = None
            ret["comment"] = "The label is going to be updated"
            return ret

        ret["comment"] = "The label is already set, changing the value"
        __salt__["kubernetes.node_add_label"](
            node_name=node, label_name=name, label_value=value, **kwargs
        )

    old_labels = copy.copy(labels)
    labels[name] = value

    ret["changes"]["{}.{}".format(node, name)] = {"old": old_labels, "new": labels}
    ret["result"] = True

    return ret
