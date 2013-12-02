# -*- coding: utf-8 -*-
"""
flask.ext.adsdata
~~~~~~~~~~~~~~

Provides interface to the adsdata metadata collections in mongodb
"""

import logging

from flask import current_app, g
from werkzeug.local import LocalProxy
from adsdata.utils import get_session
from adsdata.session import DataSession

logger = logging.getLogger(__name__)

adsdata = LocalProxy(lambda: current_app.extensions['adsdata'])    

class FlaskAdsdata(object):
    """
    Interface to the adsdata mongoalchemy session
    """
    
    def __init__(self, app=None, config=None):
        self.app = app
        self.config = config
        self.response_loader = None
        if app is not None:
            self.init_app(app, config)
            
    def init_app(self, app, config=None):
        "Initialize the session extension"

        if not (config is None or isinstance(config, dict)):
            raise ValueError("`config` must be an instance of dict or None")

        if config is None:
            config = self.config
        if config is None:
            config = app.config

        config.setdefault("ADSDATA_MONGO_DATABASE", 'adsdata')
        config.setdefault("ADSDATA_MONGO_HOST", 'localhost')
        config.setdefault("ADSDATA_MONGO_PORT", 27017)
        config.setdefault("ADSDATA_MONGO_USER", "adsdata")
        config.setdefault("ADSDATA_MONGO_PASSWORD", None)

        session = get_session(config)
        
        if not hasattr(app, 'extensions'):
            app.extensions = {}
        
        app.extensions['adsdata'] = session
        return session
    