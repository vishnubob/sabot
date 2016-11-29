#!/usr/bin/env python

import boto3
import sabot
import unittest
import uuid
from sabot import transfer

class Test_API(unittest.TestCase):
    @property
    def runid(self):
        return uuid.uuid4().hex

    @property
    def default_session(self):
        return boto3.Session()

    @property
    def session(self):
        return sabot.get_session()

    def make_bucket(self):
        cli = self.default_session.client("s3")
        name = self.runid
        cli.create_bucket(Bucket=name)
        return name

    def remove_bucket(self, name):
        cli = self.default_session.client("s3")
        cli.delete_bucket(Bucket=name)

    def test_bucket_exists(self):
        s3 = sabot.resource("s3")
        name = self.make_bucket()
        try:
            bucket = s3.Bucket(name)
            self.assertTrue(bucket.exists)
        finally:
            self.remove_bucket(name)

    def test_archive(self):
        tm = transfer.TransferManager()
        ch1 = transfer.FileReader(path="/etc/issue")
        ch2 = transfer.GzipArchive()
        ch3 = transfer.FileWriter(path="/tmp/issue.gz")
        tm[:] = (ch1, ch2, ch3)
        tm.start()
        tm.join()
        #
        ch1 = transfer.FileReader(path="/tmp/issue.gz")
        ch2 = transfer.GzipExtract()
        ch3 = transfer.FileWriter(path="/tmp/issue")
        tm[:] = (ch1, ch2, ch3)
        tm.start()
        tm.join()

if __name__ == '__main__':
    unittest.main()
