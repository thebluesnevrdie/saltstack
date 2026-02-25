# -*- coding: utf-8 -*-
'''
Module to manage Google Cloud DNS (https://docs.cloud.google.com/dns/docs/overview) zones and records
                                                                                                                                                                                      
:codeauthor:    Cody Crawford (https://github.com/thebluesnevrdie/saltstack)
:maturity:      new
:platform:      all

:depends: google-cloud-dns_ Python package

.. _google-cloud-dns: https://pypi.org/project/google-cloud-dns/

'''

import logging

import salt.utils
from salt.exceptions import CommandExecutionError, MinionError, SaltInvocationError

try:
    from google.cloud import dns
    from google.cloud.exceptions import NotFound

    HAS_GOOGLE = True
except ImportError:
    HAS_GOOGLE = False

log = logging.getLogger(__name__)


def __virtual__():
    return (
        __virtualname__
        if HAS_GOOGLE
        else (False, "Please install the google-cloud-dns Python libary from PyPI")
    )


__virtualname__ = "gdns"


def create_zone(zone_name, dns_name, description, project_id):
    client = dns.Client(project=project_id)
    zone = client.zone(
        name=zone_name,
        dns_name=dns_name,
        description=description,
    )
    zone.create()
    return zone


def get_zone(zone_name, project_id):
    client = dns.Client(project=project_id)
    zone = client.zone(name=zone_name)

    try:
        zone.reload()
        return {
            "name": zone.name,
            "dns_name": zone.dns_name,
            "description": zone.description,
        }
    except google.api_core.exceptions.NotFound:
        return None


def list_zones(project_id):
    client = dns.Client(project=project_id)

    return [zone.name for zone in client.list_zones()]


def delete_zone(zone_name, project_id):
    client = dns.Client(project=project_id)
    zone = client.zone(zone_name)
    try:
        zone.delete()
        return True
    except NotFound:
        return False


def find_record(record_name, zone_name, project_id, record_type=None, raw=False):
    client = dns.Client(project=project_id)
    try:
        zone = client.zone(zone_name)
    except google.api_core.exceptions.NotFound:
        log.debug(f"Error")
        return (False, "No such zone {zone_name}")
    zone.reload()
    for record in zone.list_resource_record_sets():
        if record.name == record_name:
            if record_type:
                if record_type.upper() != record.record_type:
                    continue
            our_record = {
                "name": record.name,
                "type": record.record_type,
                "ttl": record.ttl,
                "rrdatas": record.rrdatas,
            }
            if raw:
                return (True, our_record)
            else:
                return (
                    True,
                    __utils__["gdns.from_gdns_records"](zone.dns_name, [our_record]),
                )
    return (False, "Not found")


def list_records(zone_name, project_id):
    client = dns.Client(project=project_id)
    zone = client.zone(zone_name)
    zone.reload()
    our_zone = __utils__["gdns.to_dict_repr"](zone)
    return __utils__["gdns.from_gdns_records"](zone.dns_name, our_zone)


def list_changes(zone_name, project_id):
    client = dns.Client(project=project_id)
    zone = client.zone(zone_name)

    return [(change.started, change.status) for change in zone.list_changes()]


def make_changes(zone_name, project_id, add=[], rm=[]):
    if (not add) and (not rm):
        return "add and rm params both empty: nothing to do!"
    client = dns.Client(project=project_id)
    zone = client.zone(zone_name)
    changes = zone.changes()
    for rs in add:
        log.debug(f"rs to add: {rs}")
        add_rs = dns.ResourceRecordSet.from_api_repr(rs, zone)
        changes.add_record_set(add_rs)
    for rs in rm:
        log.debug(f"rs to rm: {rs}")
        rm_rs = dns.ResourceRecordSet.from_api_repr(rs, zone)
        changes.delete_record_set(rm_rs)
    changes.create()
    changes.reload()
    if changes.status == "done":
        return True
    else:
        return f"status: {changes.status}"
