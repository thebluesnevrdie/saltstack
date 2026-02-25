# -*- coding: utf-8 -*-
'''
Library for interacting with Google Cloud DNS (https://docs.cloud.google.com/dns/docs/overview) zones and records
                                                                                                                                                                                      
:codeauthor:    Cody Crawford (https://github.com/thebluesnevrdie/saltstack)
:maturity:      new
:platform:      all

'''

import logging

log = logging.getLogger(__name__)

DEFAULT_TTL = 3600
SINGLE_RECORD_TYPES = ["CNAME", "TLSA", "TXT"]
MULTIPLE_RECORD_TYPES = ["A", "AAAA", "CAA", "MX", "NS"]
FORBIDDEN_TYPES = []


def __virtual__():
    return True


def to_gdns_records(dns_name, records={}, soa={}):
    gdns_records = []

    if (not records) and (not soa):
        return "Records and SOA are both empty: nothing to do!"

    if soa:
        gdns_records.append(
            {
                "name": dns_name,
                "type": "SOA",
                "ttl": DEFAULT_TTL,
                "rrdatas": [
                    f"{soa['primary']} {soa['contact']} {soa['serial']} {soa['refresh']} {soa['retry']} {soa['expiration']} {soa['maxcache']}"
                ],
            }
        )

    if records:
        for rec_type, rec_data in records.items():
            for rec_entry, rec_value in rec_data.items():
                if rec_entry == "@":
                    fqdn = dns_name
                else:
                    fqdn = rec_entry + "." + dns_name
                our_gdns_entry = {
                    "name": fqdn,
                    "type": rec_type.upper(),
                    "ttl": DEFAULT_TTL,
                    "rrdatas": [],
                }

                if rec_type.upper() in SINGLE_RECORD_TYPES:
                    if rec_type.upper() == "TXT":
                        rec_value = rec_value.strip("\\n").strip("\n")
                        # https://datatracker.ietf.org/doc/html/rfc4408#section-3.1.3
                        if len(rec_value) > 256:
                            rec_value = rec_value.strip('"')
                            beg = 0
                            while beg < len(rec_value):
                                our_gdns_entry["rrdatas"].append(
                                    '"' + rec_value[beg : beg + 255] + '"'
                                )
                                beg += 255
                        else:
                            our_gdns_entry["rrdatas"].append('"' + rec_value + '"')
                    else:
                        our_gdns_entry["rrdatas"].append(rec_value)
                elif rec_type.upper() in MULTIPLE_RECORD_TYPES:
                    for this_value in rec_value:
                        our_gdns_entry["rrdatas"].append(this_value)
                else:
                    log.warning(f"Record type {rec_type} not supported, skipping...")

                gdns_records.append(our_gdns_entry)

    return gdns_records


def to_dict_repr(gdns_zone):
    this_zone = []
    for record in gdns_zone.list_resource_record_sets():
        this_zone.append(
            {
                "name": record.name,
                "type": record.record_type,
                "ttl": record.ttl,
                "rrdatas": record.rrdatas,
            }
        )
    return this_zone


def from_gdns_records(dns_name, gdns_records):
    our_records = {"soa": {}, "records": {}}
    for record in gdns_records:
        if record["type"] == "SOA":
            primary, contact, serial, refresh, retry, expiration, maxcache = record[
                "rrdatas"
            ][0].split(" ")
            our_records["soa"] = {
                "primary": primary,
                "contact": contact,
                "serial": int(serial),
                "refresh": int(refresh),
                "retry": int(retry),
                "expiration": int(expiration),
                "maxcache": int(maxcache),
            }
        else:
            if not our_records["records"].get(record["type"], None):
                our_records["records"][record["type"]] = {}
            if record["name"] == dns_name:
                our_record_name = "@"
            else:
                our_record_name = record["name"].replace("." + dns_name, "")
            if record["type"] in SINGLE_RECORD_TYPES:
                if len(record["rrdatas"]) > 1:  # this is a long TXT record
                    long_txt_rec = "".join(record["rrdatas"]).replace('"', "")
                    our_records["records"][record["type"]][
                        our_record_name
                    ] = long_txt_rec
                else:
                    txt_rec = record["rrdatas"][0].replace('"', "")
                    our_records["records"][record["type"]][our_record_name] = txt_rec
            elif record["type"] in MULTIPLE_RECORD_TYPES:
                our_records["records"][record["type"]][our_record_name] = record[
                    "rrdatas"
                ]
            else:
                log.warning(f"Record type {record['type']} not supported, skipping...")
    if not our_records["soa"]:
        del our_records["soa"]
    if not our_records["records"]:
        del our_records["records"]

    return our_records
