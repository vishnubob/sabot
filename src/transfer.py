import threading
import multiprocessing
import time
import tarfile
import os
import uuid
import zlib
import bz2
import sys

from . import log

logger = log.get_logger(__name__)

def f_walkpath(path):
    for (root, dirs, files) in os.walk(path):
        for name in itertools.chain(files, dirs):
            yield os.path.join(root, name)

def f_arcpath(path, arcpath=None, relpath=None):
    if relpath:
        path = os.path.relpath(path, relpath)
    if arcpath:
        path = os.path.join(arcpath, path)
    return path

class TransferManager(list):
    def plumb_workers(self):
        self.pipes = PipeManager()
        last_worker = self[0]
        for worker in self[1:]:
            self.pipes.connect(last_worker, worker)
            last_worker = worker

    def close_pipes(self):
        self.pipes.close("__root__")

    def start_workers(self):
        for worker in self:
            worker.start()

    def start(self):
        self.plumb_workers()
        self.start_workers()
        self.close_pipes()

    def join(self):
        for worker in self:
            worker.join()

class TransferPipe(object):
    def __init__(self, manager):
        (rp, wp) = os.pipe()
        self.pipe_read = os.fdopen(rp)
        self.pipe_write = os.fdopen(wp, 'w')
        self.pipe_manager = manager

    def close(self, name=None):
        self.pipe_manager.close(name)

    def close_write(self, name=''):
        if not self.pipe_write.closed: 
            #print "%s: closing write pipe %d" % (name, self.pipe_write.fileno())
            self.pipe_write.close()

    def close_read(self, name=''):
        if not self.pipe_read.closed:
            #print "%s: closing read pipe %d" % (name, self.pipe_read.fileno())
            self.pipe_read.close()

    def write(self, data):
        self.pipe_write.write(data)

    def read(self, bufsize=None):
        if bufsize:
            return self.pipe_read.read(bufsize)
        return self.pipe_read.read()

class PipeManager(object):
    def __init__(self):
        self.read_pipes = {}
        self.write_pipes = {}

    def connect(self, source, target):
        pipe = TransferPipe(self)
        source.endpoint_bind(pipe_write=pipe)
        assert source.name not in self.write_pipes
        self.write_pipes[source.name] = pipe
        target.endpoint_bind(pipe_read=pipe)
        assert target.name not in self.read_pipes
        self.read_pipes[target.name] = pipe

    def close(self, name=None):
        assert name != None
        rplist = [rp for (nm, rp) in self.read_pipes.items() if nm != name]
        for rp in rplist:
            rp.close_read(name)
        wplist = [rp for (nm, rp) in self.write_pipes.items() if nm != name]
        for wp in wplist:
            wp.close_write(name)

class Endpoint(object):
    def __init__(self, *args, **kw):
        self.pipe_read = None
        self.pipe_write = None

    def endpoint_bind(self, pipe_read=None, pipe_write=None):
        if pipe_read != None:
            self.pipe_read = pipe_read
        if pipe_write != None:
            self.pipe_write = pipe_write

    def endpoint_init(self):
        if self.pipe_write:
            self.pipe_write.close(self.name)
        if self.pipe_read:
            self.pipe_read.close(self.name)

    def endpoint_finalize(self):
        if self.pipe_write:
            self.pipe_write.close("final")
        if self.pipe_read:
            self.pipe_read.close("final")

    def write(self, data):
        self.pipe_write.write(data)

    def read(self, bufsize=None):
        bufsize = bufsize if bufsize != None else self.bufsize
        return self.pipe_read.read(bufsize)

class TransferWorker(Endpoint, multiprocessing.Process):
    Defaults = {
        "bufsize": 2 ** 16,
    }

    def __init__(self, **kw):
        # name
        tid = str(uuid.uuid4()).split('-')[0]
        name = "%s-%s" % (self.__class__.__name__, tid)
        Endpoint.__init__(self)
        multiprocessing.Process.__init__(self, name=name)
        # defaults
        defaults = self.get_defaults()
        for key in defaults:
            val = kw.get(key, defaults[key])
            setattr(self, key, val)

    def get_defaults(self):
        clist = self.__class__.mro()
        clist = clist[::-1]
        defaults = {}
        for cls in clist:
            defs = getattr(cls, "Defaults", {})
            defaults.update(defs)
        return defaults

    def start(self):
        self.daemon = True
        self.transfer_count = 0
        super(TransferWorker, self).start()

    def run(self):
        #print "%s: running" % self.name
        self.endpoint_init()
        try:
            self.transfer()
        finally:
            self.endpoint_finalize()

    def transfer_callback(self, bytecount):
        self.transfer_count += bytecount

class UriSource(TransferWorker):
    Defaults = {
        "uri": None,
    }

    def transfer(self):
        r = requests.get(self.uri)
        for chunk in r.iter_content(chunk_size=self.bufsize):
            self.write(chunk)

class TarArchive(TransferWorker):
    Defaults = {
        "manifest": None,
        "arcpath": None,
        "relapth": None,
    }

    def transfer(self):
        tf = tarfile.TarFile(mode='w', fileobj=self.endpoint)
        for path in self.manifest:
            arcname = f_arcpath(arcpath=self.arcpath, relpath=self.relpath)
            tarfile.add(path, arcname=arcname)

class TarExtract(TransferWorker):
    Defaults = {
        "path": None,
        "mode": 'r',
    }

    def transfer(self):
        tf = tarfile.TarFile(mode='r', fileobj=self.endpoint)
        tarfile.extractall(path=self.path)

class GzipArchive(TransferWorker):
    def transfer(self):
        engine = zlib.compressobj()
        while 1:
            data = self.read()
            if not data:
                break
            self.write(engine.compress(data))
        self.write(engine.flush(zlib.Z_FINISH))

class GzipExtract(TransferWorker):
    def transfer(self):
        engine = zlib.decompressobj()
        while 1:
            data = self.read()
            if not data:
                break
            output = engine.decompress(data)
            self.write(output)
        self.write(engine.flush())

class Bz2Extract(TransferWorker):
    def transfer(self):
        engine = bz2.BZ2Decompressor()
        while 1:
            data = self.read()
            if not data:
                break
            self.write(engine.decompress(data))

class Bz2Archive(TransferWorker):
    def transfer(self):
        engine = bz2.BZ2Compressor()
        while 1:
            data = self.read()
            if not data:
                break
            self.write(engine.compress(data))
        self.write(engine.flush())

class FileReader(TransferWorker):
    Defaults = {
        "path": None,
        "mode": "rb",
    }

    def transfer(self):
        with open(self.path, self.mode) as fh:
            while 1:
                data = fh.read(self.bufsize)
                if not data:
                    break
                self.write(data)

class FileWriter(TransferWorker):
    Defaults = {
        "path": None,
        "mode": "wb",
    }

    def transfer(self):
        with open(self.path, self.mode) as fh:
            while 1:
                data = self.read()
                if not data:
                    break
                fh.write(data)

class S3UploadWorker(TransferWorker):
    def transfer(self):
        s3obj.upload_fileobj(self.endpoint, Callback=self.transfer_callback)

class S3DownloadWorker(TransferWorker):
    def transfer(self):
        s3obj.download_fileobj(self.endpoint, Callback=self.transfer_callback)

archive_map = {
    'tar': ('tar',),
    'tar.gz': ('tar', 'gz'),
    'tgz': ('tar', 'gz'),
    'tar.bz2': ('tar', 'bz2'),
    'tbz2': ('tar', 'bz2'),
    'gz': ('gz',),
    'bz2': ('bz2',),
    'zip': ('zip'),
}

def archive_factory(mode='r', ext="tar.gz"):
    chain = archive_map.get(ext.lower(), None)
    if chain == None:
        msg = "undiscernible archive format '%s'" % ext.lower()
        raise ValueError(msg)
    if 'tar' in chain:
        if 'gz' in chain:
            mode = mode + "|gz"
        elif 'bz2' in chain:
            mode = mode + "|bz2"

def upload_factory(s3obj, ext="tar.gz", *args, **kw):
    (pipe_read, pipe_write) = pipe_factory()
    args = (s3obj, pipe_read)
    chain = [TransferWorker(args)]
    if 'tar' in ext:
        tar_factory(ext=ext)
