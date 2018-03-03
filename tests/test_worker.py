from unittest import TestCase
import etcd
import uuid
from pyetcdq import Worker, Client


class WorkerTestCase(TestCase):
    def setUp(self):
        self.etcd_client = etcd.Client(port=2379)
        self.prefix = "/ut-%s" % uuid.uuid4()

        self.worker = Worker(self.etcd_client, self.prefix)
        self.client = Client(self.etcd_client, self.prefix)
        self.worker.start()

    def tearDown(self):
        self.worker.stop()

        # clean up after each run
        try:
            self.etcd_client.delete(self.prefix, recursive=True)
        except:
            pass

    def test_run_task(self):
        t1 = self.client.submit({"test": 1})

        check = self.worker.wait_for_task(timeout=5)
        self.assertEqual(t1.tid, check.tid)

        self.worker.finish_task(check)

    def test_wait_timeout(self):
        with self.assertRaisesRegexp(RuntimeError, "Timeout"):
            self.worker.wait_for_task(timeout=2)
