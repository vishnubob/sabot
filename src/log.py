from logging import *

rootlog = None
def _bootstrap():
    global rootlog
    if rootlog != None:
        return
    rootlog = getLogger("sabot")
    rootlog.setLevel(DEBUG)
    ch = StreamHandler()
    ch.setLevel(INFO)
    formatter = Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    rootlog.addHandler(ch)
_bootstrap()

def get_logger(name):
    return getLogger(name)
