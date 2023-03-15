"""
Adjusts k3s kubernetes configuration so that k3s cluster is accessible from airflow.
"""

import yaml

with open('kubeconfig.yaml', 'r') as fp, open('airflow-kube.yaml', 'w') as outfp:
    data = yaml.safe_load(fp)
    data['clusters'][0]['cluster']['server'] = 'https://k3s-server:6443'
    yaml.dump(data, outfp)
