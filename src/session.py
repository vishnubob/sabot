import boto3
from . hooks import get_hooks

__all__ = ["get_session"]

class SessionManager(object):
    DefaultSessionName = "__default__"
    Sessions = {}

    def _build_session(self):
        session = boto3.Session()
        for hook in get_hooks():
            session.events.register(*hook)
        return session

    def get_session(self, name=None):
        name = name if name != None else self.DefaultSessionName
        if name not in self.Sessions:
            self.Sessions[name] = self._build_session()
        return self.Sessions[name]

    def resource(self, name):
        session = get_session()
        return session.resource(name)

    def client(self, name):
        session = get_session()
        return session.client(name)

def get_session(*args, **kw):
    sm = SessionManager()
    return sm.get_session(*args, **kw)

def resource(*args, **kw):
    sm = SessionManager()
    return sm.resource(*args, **kw)

def client(*args, **kw):
    sm = SessionManager()
    return sm.client(*args, **kw)

def bind_session(target):
    def bind_target(*args, **kw):
        #defs = dict(zip(target.func_code.co_varnames[::-1], target.func_defaults[::-1]))
        #bind_session = kw.pop("bind_session", None)
        session = kw.get("session", None)
        kw["session"] = session if session != None else get_session()
        target(*args, **kw)
