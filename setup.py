#!/usr/bin/env python

from setuptools import setup
from pip.req import parse_requirements

# parse_requirements() returns generator of pip.req.InstallRequirement objects
install_reqs = parse_requirements('./requirements.txt')
reqs = [str(ir.req) for ir in install_reqs]

setup(
    name = 'adsdata',
    version = '0.2',
    description = 'ADS metadata management',
    long_description = open('README.md').read(), 
    install_requires = reqs,
    author = 'Jay Luker',
    author_email = 'jluker@cfa.harvard.edu',
    url = 'http://github.com/adsabs/adsdata',
    packages = [
        'adsdata',
    ],
    classifiers = [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Operating System :: POSIX :: Linux',
        'Topic :: Scientific/Engineering :: Astronomy',
        'Topic :: Internet :: WWW/HTTP :: Indexing/Search',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Software Development :: Libraries'    ]
)
