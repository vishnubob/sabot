#!/usr/bin/env python

import os
import shutil
import unittest
import uuid
import random
import filecmp

import boto3
from sabot import transfer
import sabot

def random_tag():
    return str(uuid.uuid4()).split('-')[0]

class MockDirectory(object):
    Limits = {
        'dir_count': 10,
        'file_count': 30,
        'min_size': 1024,
        'max_size': 4096,
        'symbols': 20,
    }

    def __init__(self, root="/tmp"):
        self.root = os.path.join(root, random_tag())
        os.mkdir(self.root)
        self.dir_stack = []
        self.limits = self.Limits.copy()
        self.build()

    def __del__(self):
        if os.path.exists(self.root):
            shutil.rmtree(self.root)

    @property
    def manifest(self):
        return transfer.Manifest(self.root, relpath=self.root)

    def compare(self, path):
        res = filecmp.dircmp(self.root, path)
        return not (bool(res.left_only) or bool(res.right_only))
    
    def is_finished(self):
        counts = [v for (k, v) in self.limits.items() if k.endswith("_count")]
        counts = sum(counts)
        return counts <= 0

    def _file(self):
        filename = random_tag()
        path = [self.root] + self.dir_stack + [filename]
        path = os.path.join(*path)
        sz = random.randint(self.limits["min_size"], self.limits["max_size"])
        payload = str.join('', [chr(random.randint(0, self.limits["symbols"])) for ch in range(sz)])
        with open(path, 'wb') as fh:
            fh.write(payload)
        self.limits["file_count"] -= 1

    def _push(self):
        dirname = random_tag()
        self.dir_stack.append(dirname)
        path = [self.root] + self.dir_stack
        os.mkdir(os.path.join(*path))
        self.limits["dir_count"] -= 1

    def _pop(self):
        self.dir_stack = self.dir_stack[:-1]

    def build(self):
        opts = ["_push", "_file", "_pop"]
        while not self.is_finished():
            cmd = random.choice(opts)
            getattr(self, cmd)()

class Test_API(unittest.TestCase):
    @property
    def runid(self):
        return random_tag()

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

    def remove_bucket(self, bucketname):
        cli = self.default_session.client("s3")
        while 1:
            resp = cli.list_objects_v2(Bucket=bucketname)
            inventory = resp.get("Contents", [])
            keys = [item["Key"] for item in inventory]
            for keyname in keys:
                cli.delete_object(Bucket=bucketname, Key=keyname)
            if not resp["IsTruncated"]:
                break
        cli.delete_bucket(Bucket=bucketname)

    def test_bucket_exists(self):
        s3 = sabot.resource("s3")
        name = self.make_bucket()
        try:
            bucket = s3.Bucket(name)
            self.assertTrue(bucket.exists)
        finally:
            self.remove_bucket(name)

    def test_tar_archive(self):
        s3 = sabot.resource("s3")
        mock = MockDirectory()
        downpath = os.path.join("/tmp", random_tag())
        key = random_tag()
        try:
            bucket_name = self.make_bucket()
            s3obj = s3.Object(bucket_name, key)
            s3obj.upload(manifest=mock.manifest).join()
            s3obj.wait_until_exists()
            s3obj.download(path=downpath).join()
            self.assertTrue(mock.compare(downpath))
        finally:
            self.remove_bucket(bucket_name)
            if os.path.exists(downpath):
                shutil.rmtree(downpath)

if __name__ == '__main__':
    unittest.main()
