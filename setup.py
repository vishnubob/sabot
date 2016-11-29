#!/usr/bin/env python

from setuptools import setup, find_packages
from pip.req import parse_requirements

sctk = {
    "name": "sabot",
    "description": "boto3 extension classes",
    "author": "Giles Hall",
    "author_email": "giles@polymerase.org",
    "keywords": ["boto3", "aws"],
    "packages": find_packages(),
    "version": "0.1",
    "scripts": [],
}

def populate_requirements(conf, reqfn=None):
    reqfn = reqfn if reqfn != None else "requirements.txt" 
    install_reqs = list(parse_requirements(reqfn, session=False))
    _reqs = conf.get("install_requires", [])
    _deps = conf.get("dependency_links", [])
    _reqs.extend([str(ir.req) for ir in install_reqs])
    if _reqs:
        conf["install_requires"] = _reqs
    try:
        _deps.extend([str(ir.link.url) for ir in install_reqs if ir.link])
    except AttributeError:
        _deps.extend([str(ir.url + '#egg=' + str(ir.req)) for ir in install_reqs if ir.url])
    if _deps:
        conf["dependency_links"] = _deps
    return conf

if __name__ == "__main__":
    conf = sctk.copy()
    conf = populate_requirements(conf)
    setup(**conf)
