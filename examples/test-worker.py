import etcd
import time
from pyetcdq import Worker

client = etcd.Client(port=2379)
# client.delete('/test', recursive=True)

worker = Worker(client, '/test')
worker.start()

try:
    print ("Worker: %s" % worker._worker_key)

    while True:
        task = worker.wait_for_task()
        print("process %s" % task)
        time.sleep(5)
        worker.finish_task(task)
finally:
    worker.stop()
