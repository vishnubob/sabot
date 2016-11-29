import sys
import botocore
import inspect
import os

__all__ = ["get_hooks"]

class SabotHook(object):
    EventHook = None

    @classmethod
    def install_hook(cls, base_classes, **kw):
        base_classes.insert(0, cls)

class SabotObjectS3(SabotHook):
    EventHook = "creating-resource-class.s3.Object"

    def upload(self, *args, **kw):
        pass

class SabotBucket(SabotHook):
    EventHook = "creating-resource-class.s3.Bucket"

    @property
    def exists(self):
        try:
            self.meta.client.head_bucket(Bucket=self.name)
        except botocore.exceptions.ClientError as err:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(err.response['Error']['Code'])
            if error_code == 404:
                return False
            raise
        return True

    def upload(self, content, mode=None, meta=None, extra=None, block=True):
        assert block, "non-blocking is not implemented yet"
        # what is content?
        if issubclass(content, basestring):
            # is it a path?
            if os.path.exists(content):
                # is it a directory?
                pass


def get_hooks():
    this = sys.modules[__name__]
    for (name, obj) in inspect.getmembers(this):
        if not inspect.isclass(obj):
            continue
        if not issubclass(obj, SabotHook):
            continue
        if obj.EventHook == None:
            continue
        yield (obj.EventHook, obj.install_hook)
