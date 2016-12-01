import threading
import multiprocessing
import time
import tarfile
import os
import uuid
import zlib
import bz2
import sys
import itertools

from . import log

logger = log.get_logger(__name__)

class Manifest(object):
    def __init__(self, path, callback=None, arcpath=None, relpath=None):
        self.path = path
        self.callback = callback
        self.arcpath = arcpath
        self.relpath = relpath

    def walk(self):
        if os.path.isdir(self.path):
            for (root, dirs, files) in os.walk(self.path):
                for name in itertools.chain(files, dirs):
                    path = os.path.join(root, name)
                    yield path
        else:
            yield self.path

    def get_size(self):
        sz = 0
        for (path, arcname) in self:
            st = os.stat(path)
            sz += st.st_size

    def arcname(self, path):
        if self.relpath:
            path = os.path.relpath(path, self.relpath)
        if self.arcpath:
            path = os.path.join(self.arcpath, path)
        return path

    def __iter__(self):
        for path in self.walk():
            if self.callback and not self.callback(path):
                continue
            arcname = self.arcname(path)
            yield (path, arcname)

class PipeManager(object):
    def __init__(self):
        self.pipes = {}
        self.pipe_count = 0

    def _add_pipe(self, worker, mode, pipe):
        if worker.name not in self.pipes:
            self.pipes[worker.name] = {}
        assert mode not in self.pipes[worker.name]
        self.pipes[worker.name][mode] = pipe
        kw = {mode: pipe}
        worker.endpoint_bind(**kw)

    def connect(self, source, target):
        pipe = TransferPipe(self)
        self._add_pipe(source, "write", pipe)
        self._add_pipe(target, "read", pipe)
        self.pipe_count += 1

    def close(self, name=None):
        pipelist = [pp for (nm, pp) in self.pipes.items() if nm != name]
        for pp in pipelist:
            if "read" in pp:
                pp["read"].close_read()
            if "write" in pp:
                pp["write"].close_write()

class TransferManager(list):
    def __init__(self, *args):
        self[:] = args

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

    def close_write(self):
        if not self.pipe_write.closed: 
            self.pipe_write.close()

    def close_read(self):
        if not self.pipe_read.closed:
            self.pipe_read.close()

    def write(self, data):
        self.pipe_write.write(data)

    def read(self, bufsize=None):
        if bufsize:
            return self.pipe_read.read(bufsize)
        return self.pipe_read.read()
    
class Throughput(object):
    def __init__(self):
        self.counter = 0
        self.start_time = None

    def update(self, size):
        now = time.time()
        if self.start_time == None:
            self.start_time = now
        self.counter += size

    @property
    def throughput(self):
        if self.start_time == None:
            return 0
        delta = now - self.start_time
        if delta <= 0:
            return 0
        return self.counter / float(delta)

class Endpoint(object):
    def __init__(self, *args, **kw):
        self.pipe_read = None
        self.pipe_write = None
        self.pipe_read_throughput = Throughput()
        self.pipe_write_throughput = Throughput()

    def endpoint_bind(self, read=None, write=None):
        if read != None:
            self.pipe_read = read
        if write != None:
            self.pipe_write = write

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
        self.pipe_write_throughput.update(len(data))

    def read(self, bufsize=None):
        bufsize = bufsize if bufsize != None else self.bufsize
        data = self.pipe_read.read(bufsize)
        self.pipe_read_throughput.update(len(data))
        return data

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
    }

    def transfer(self):
        tf = tarfile.TarFile.open(mode='w|', fileobj=self.pipe_write)
        for (path, arcname) in self.manifest:
            tf.add(path, arcname=arcname)

class TarExtract(TransferWorker):
    Defaults = {
        "path": ".",
        "mode": 'r',
    }

    def transfer(self):
        tf = tarfile.TarFile.open(mode='r|', fileobj=self.pipe_read)
        tf.extractall(path=self.path)

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

class Bzip2Archive(TransferWorker):
    def transfer(self):
        engine = bz2.BZ2Compressor()
        while 1:
            data = self.read()
            if not data:
                break
            self.write(engine.compress(data))
        self.write(engine.flush())

class Bzip2Extract(TransferWorker):
    def transfer(self):
        engine = bz2.BZ2Decompressor()
        while 1:
            data = self.read()
            if not data:
                break
            self.write(engine.decompress(data))

class FileReaderWorker(TransferWorker):
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

class FileWriterWorker(TransferWorker):
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
    Defaults = {
        "s3obj": None,
        "ExtraArgs": None,
    }

    def transfer(self):
        self.s3obj.upload_fileobj(self.pipe_read, ExtraArgs=self.ExtraArgs, Callback=self.transfer_callback)

class S3DownloadWorker(TransferWorker):
    Defaults = {
        "s3obj": None,
        "ExtraArgs": None,
    }

    def transfer(self):
        self.s3obj.download_fileobj(self.pipe_write, ExtraArgs=self.ExtraArgs, Callback=self.transfer_callback)

class TransferFactory(object):
    ArchiveChainMap = {
        'tar': ('tar',),
        'tar.gz': ('tar', 'gz'),
        'tgz': ('tar', 'gz'),
        'tar.bz2': ('tar', 'bz2'),
        'tbz2': ('tar', 'bz2'),
        'gz': ('gz',),
        'bz2': ('bz2',),
        'zip': ('zip'),
    }
    ArchiveMap = {
        "tar": TarArchive,
        "gz": GzipArchive,
        "bz2": Bzip2Archive,
        "zip": None,
    }
    ExtractMap = {
        "tar": TarExtract,
        "gz": GzipExtract,
        "bz2": Bzip2Extract,
        "zip": None,
    }

    def extract_chain(self, archive=None, **kw):
        if archive == None:
            return []
        cart = self.ArchiveChainMap.get(archive, None)
        if cart == None:
            msg = "undiscernible archive format '%s'" % ext
            raise ValueError(msg)
        chain = [self.ExtractMap[cn](**kw) for cn in cart[::-1]]
        return chain

    def archive_chain(self, archive=None, **kw):
        if archive == None:
            return []
        cart = self.ArchiveChainMap.get(archive, None)
        if cart == None:
            msg = "undiscernible archive format '%s'" % ext
            raise ValueError(msg)
        chain = [self.ArchiveMap[cn](**kw) for cn in cart]
        return chain

    def upload(self, path=None, manifest=None, s3obj=None, relpath=None, arcpath=None, archive=None, **kw):
        if path and manifest:
            raise ValueError("You can only specify a path or a manifest")
        if path:
            if not os.path.exists(path):
                raise ValueError("path does not exist: %s" % path)
            if os.path.isdir(path):
                if not recursive:
                    raise ValueError("uploading directories requires recursive flag")
                manifest = Manifest(path, relpath=relpath, arcpath=arcpath)
            else:
                archive = archive if archive != None else "bz2"
                chain = self.archive_chain(archive)
                chain = [FileReaderWorker(path=path)] + chain
        if manifest:
            archive = archive if archive != None else "tar.bz2"
            chain = self.archive_chain(archive, manifest=manifest)
        extra = {
            "Metadata": {
                "__manifest__": str((manifest != None)),
                "__archive__": str(archive),
            }
        }
        chain = chain + [S3UploadWorker(s3obj=s3obj, ExtraArgs=extra)]
        tm = TransferManager(*chain)
        tm.start()
        return tm
            
    def download(self, s3obj=None, archive=None, **kw):
        archive = archive if archive != None else s3obj.metadata.get("__archive__", None)
        manifest_flag = s3obj.metadata.get("__manifest__", False)
        chain = self.extract_chain(archive, **kw)
        chain = [S3DownloadWorker(s3obj=s3obj)] + chain
        if not manifest_flag or not archive:
            chain = chain + [FileWriterWorker(**kw)]
        tm = TransferManager(*chain)
        tm.start()
        return tm

def upload(*args, **kw):
    return TransferFactory().upload(*args, **kw)

def download(*args, **kw):
    return TransferFactory().download(*args, **kw)
