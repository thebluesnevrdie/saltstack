# -*- coding: utf-8 -*-
'''
Module to manage Google Cloud DNS (https://docs.cloud.google.com/dns/docs/overview) zones and records
                                                                                                                                                                                      
:codeauthor:    Cody Crawford (https://github.com/thebluesnevrdie/saltstack)
:maturity:      new
:platform:      all


Example:

.. code-block:: yaml

    Manage example.com zone:
      gdns.zone_managed:
        - name: example-com
        - project_id: codycrawford-test
        - dns_name: 'example.com'
        - description: "This is the example zone"
        - soa:
            contact: ops.exact.zero
            expiration: 259200
            maxcache: 300
            primary: ns-cloud-c1.googledomains.com
            refresh: 21600
            retry: 3600
            serial: 1
        - records:
            A:
              cody:
                - 10.11.12.13
                - 10.9.8.7
              cody2:
                - 10.99.66.1
            CNAME:
              salt: cfg.
            MX:
              '@':
                - 10 mx1.mail.com.
                - 20 mx2.mail.org.
            NS:
              '@':
                - ns-cloud-c1.googledomains.com.
                - ns-cloud-c2.googledomains.com.
                - ns-cloud-c3.googledomains.com.
                - ns-cloud-c4.googledomains.com.
            TXT:
              '@': 'v=spf1 include:spf.protection.outlook.com include:20929816.spf02.hubspotemail.net -all'
              atlassian-domain-verification: 'Rhg4wUymWGpOAfh0zpCGGvYnfNar21wnvCafu/5kqavbInc5avPQyrldv4tPsWBq'
              reallylongtxtrecord: |
                Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ullamcorper sit amet risus nullam eget felis eget nunc lobortis. Adipiscing commodo elit at imperdiet. Enim nulla aliquet porttitor lacus luctus accumsan tortor posuere ac. Sit amet consectetur adipiscing elit duis. Cursus sit amet dictum sit amet justo donec enim diam. Porta non pulvinar neque laoreet suspendisse. Netus et malesuada fames ac. Sodales ut eu sem integer vitae justo. Consequat interdum varius sit amet mattis vulputate. Porttitor leo a diam sollicitudin tempor. Non tellus orci ac auctor augue mauris. Quis blandit turpis cursus in hac.

    Ensure example.org zone is gone:
      gdns.zone_absent:
        - name: example-org
        - project_id: codycrawford-test

    Ensure this.example.org record is gone:
      gdns.record_absent:
        - name: this.example.org.
        - zone: example-org
        - project_id: codycrawford-test


'''

import logging

import salt.utils.files
from salt.utils.dictdiffer import deep_diff
import salt.utils.dictupdate as dictupdate
from salt.exceptions import CommandExecutionError

log = logging.getLogger(__name__)


def __virtual__():
    return "gdns"


class DictDiffer(object):
    """

    Calculate the difference between two dictionaries as:
    (1) items added
    (2) items removed
    (3) keys same in both but changed values
    (4) keys same in both and unchanged values

    """

    def __init__(self, past_dict, current_dict):
        self.current_dict, self.past_dict = current_dict, past_dict
        self.set_current, self.set_past = set(current_dict.keys()), set(
            past_dict.keys()
        )
        self.intersect = self.set_current.intersection(self.set_past)

    def added(self):
        return self.set_current - self.intersect

    def removed(self):
        return self.set_past - self.intersect

    def changed(self):
        return set(
            o for o in self.intersect if self.past_dict[o] != self.current_dict[o]
        )

    def unchanged(self):
        return set(
            o for o in self.intersect if self.past_dict[o] == self.current_dict[o]
        )


def zone_managed(name, dns_name, soa, project_id, description=None, records=None):
    ret = {"name": name, "changes": {}, "result": False, "comment": ""}

    add_records = []
    rm_records = []
    have_changes = False

    if records:
        if records.get("TXT", None):
            for txt_k, txt_v in records["TXT"].items():
                records["TXT"][txt_k] = txt_v.strip("\\n").strip("\n")

    if name not in __salt__["gdns.list_zones"](project_id):
        created = __salt__["gdns.create_zone"](name, dns_name, description, project_id)

        # Zones always have SOA and NS records, so we must remove the default ones
        rm_records.append(
            __salt__["gdns.find_record"](
                dns_name, name, project_id, record_type="SOA", raw=True
            )[1]
        )
        rm_records.append(
            __salt__["gdns.find_record"](
                dns_name, name, project_id, record_type="NS", raw=True
            )[1]
        )

        add_records = __utils__["gdns.to_gdns_records"](
            dns_name, records=records, soa=soa
        )

        ret["changes"] = {"old": None, "new": f"Zone {name} created."}
        have_changes = True
    else:
        existing = __salt__["gdns.list_records"](name, project_id)
        if existing["soa"] != soa:
            have_changes = True
            # SOA is only changed, never added nor removed
            rm_records.append(
                __salt__["gdns.find_record"](
                    dns_name, name, project_id, record_type="SOA", raw=True
                )[1]
            )
            add_records.append(__utils__["gdns.to_gdns_records"](dns_name, soa=soa)[0])
        # compare the records - will always contain NS
        if existing["records"] != records:
            have_changes = True
            type_diff = DictDiffer(existing["records"], records)
            # We need to add all records for an entire record type
            for rr in type_diff.added():
                for added in records[rr]:
                    add_records.append(
                        __utils__["gdns.to_gdns_records"](
                            dns_name, records={rr: {added: records[rr][added]}}
                        )[0]
                    )
            # We need to remove all records for an entire record type
            for rr in type_diff.removed():
                # Zone must contain at least one NS record
                if rr == "NS":
                    ret["comment"] = "Error: cannot remove all NS records!"
                    ret["result"] = False
                    return ret
                for removed in existing["records"][rr]:
                    rm_records.append(
                        __utils__["gdns.to_gdns_records"](
                            dns_name,
                            records={rr: {removed: existing["records"][rr][removed]}},
                        )[0]
                    )
            # We need to add/remove/change records for each record type
            for rr in type_diff.changed():
                entry = DictDiffer(existing["records"][rr], records[rr])
                for added in entry.added():
                    add_records.append(
                        __utils__["gdns.to_gdns_records"](
                            dns_name, records={rr: {added: records[rr][added]}}
                        )[0]
                    )
                for removed in entry.removed():
                    log.debug(f"removed: {removed}")
                    rm_records.append(
                        __utils__["gdns.to_gdns_records"](
                            dns_name,
                            records={rr: {removed: existing["records"][rr][removed]}},
                        )[0]
                    )
                for changed in entry.changed():
                    log.debug(f"changed: {changed}")
                    add_records.append(
                        __utils__["gdns.to_gdns_records"](
                            dns_name, records={rr: {changed: records[rr][changed]}}
                        )[0]
                    )
                    rm_records.append(
                        __utils__["gdns.to_gdns_records"](
                            dns_name,
                            records={rr: {changed: existing["records"][rr][changed]}},
                        )[0]
                    )

    if have_changes:
        update = __salt__["gdns.make_changes"](
            name, project_id, add=add_records, rm=rm_records
        )
        log.debug(f"add_records: {add_records}")
        if not ret["changes"]:
            dictupdate.update(
                ret["changes"], deep_diff(existing, {"soa": soa, "records": records})
            )
    ret["result"] = True
    return ret


def zone_absent(name, project_id):
    ret = {"name": name, "changes": {}, "result": False, "comment": ""}

    zones = __salt__["gdns.list_zones"](project_id)
    if name in zones:
        deleted = __salt__["gdns.delete_zone"](name, project_id)
        if deleted:
            ret["changes"] = {"old": "present", "new": None}
            ret["comment"] = f"Zone {name} was deleted"
        else:
            ret["comment"] = f"Failed to remove zone: {name}"
            return ret
    else:
        ret["comment"] = f"Zone {name} does not exist"
    ret["result"] = True
    return ret


def record_absent(name, zone, project_id):
    ret = {"name": name, "changes": {}, "result": False, "comment": ""}

    our_record = __salt__["gdns.find_record"](name, zone, project_id)
    if our_record[0]:
        log.debug(f"our_record = {our_record[1]}")
        __salt__["gdns.make_changes"](zone, project_id, rm=[our_record[1]])
        ret["result"] = True
        ret["comment"] = ""
        ret["changes"] = {"old": "present", "new": None}
    else:
        ret["result"] = True
        ret["comment"] = "Record not found."
    return ret
