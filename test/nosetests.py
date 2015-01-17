#!/usr/bin/env python
'''
Created on Nov 26, 2012

@author: jluker
'''
import sys
import nose

config = nose.config.Config()
config.addPaths = False

nose.main(config=config)