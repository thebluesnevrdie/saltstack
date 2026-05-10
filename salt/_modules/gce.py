# -*- coding: utf-8 -*-
'''
Module for getting IP address details from Google Cloud

'''

import json
import salt.utils

def __virtual__():
    return True

def get_addresses():
    ips = {}
    our_cmd = 'gcloud compute addresses list --format=json'
    cmd_out = __salt__['cmd.run_stdout'](our_cmd)
    if not len(cmd_out): return {}
    our_data = json.loads(cmd_out)
    for item in our_data:
        ips[item["name"]] = {
            "address": item["address"],
            "addressType": item["addressType"],
            "networkTier": item["networkTier"],
            "status": item["status"]
            }
        if item.get("purpose", None):
            ips[item["name"]]["purpose"] = item["purpose"]
    return ips
