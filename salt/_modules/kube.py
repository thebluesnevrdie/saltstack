# -*- coding: utf-8 -*-
'''
Module for getting ingress details in a Kubernetes cluster
                                                                                                                                                                                      
:codeauthor:    Cody Crawford (https://github.com/thebluesnevrdie/saltstack)
:maturity:      new
:platform:      all
'''

import json
import salt.utils

kubectl = '/usr/bin/kubectl'

def __virtual__():
    return True

def get_ingress():
    if not salt.utils.files.is_binary(kubectl): return []
    endpoints = []
    our_cmd = f'{kubectl} -o json get ingress -A'
    cmd_out = __salt__['cmd.run_stdout'](our_cmd)
    if not len(cmd_out): return []
    our_data = json.loads(cmd_out)
    for item in our_data['items']:
        fqdn = item['spec']['rules'][0]['host']
        domain = '.'.join(fqdn.split('.')[-2:])
        hostname = '.'.join(fqdn.split('.')[0:-2])
        if not item['status']['loadBalancer']:
            continue
        ip = item['status']['loadBalancer']['ingress'][0]['ip']
        endpoints.append({'hostname': hostname, 'domain': domain, 'ip': ip})
    return endpoints
