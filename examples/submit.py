import etcd
from pyetcdq.client import Client

etcd_client = etcd.Client(port=2379)
client = Client(etcd_client, '/test')

print client.submit({'test': 1})
