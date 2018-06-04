# -*- coding: utf-8 -*-
# Filename: tools.py
"""
frontendServer - render macsServer database to web page
=======================================================

Copyright (C) 2013 Tobias Kölling
"""

import tornado.web
import urlparse
import hashlib

https_hostnames = ['macsserver.physik.uni-muenchen.de',
                   'macsserver.physik.lmu.de']

class RequestHandler(tornado.web.RequestHandler):
    conservativeEtagGeneration = False
    def staticFile(self, fn):
        return self.serverPrefix + "/static/" + fn
    def initialize(self, *args, **kwargs):
        self.serverPrefix = self.request.headers.get('X-Server-Prefix', '')
        self.host = self.request.headers.get('X-Forwarded-Host', self.request.host)
        if self.host in https_hostnames:
            self.protocol = 'https'
            self.dap_requires_key = True
        else:
            self.protocol = 'http'
            self.dap_requires_key = False
    def compute_etag(self):
        if self.conservativeEtagGeneration or self.request.method != 'GET':
            return super(RequestHandler, self).compute_etag()
        else:
            uri = self.request.uri
            uriparts = list(urlparse.urlparse(uri))
            query = uriparts[4].split('&')
            query = [x for x in query if not x.startswith('key=')]
            uriparts[4] = '&'.join(query)
            uri = urlparse.urlunparse(uriparts)
            gitrev = self.settings['git-rev']
            return hashlib.sha256('!'.join((uri, gitrev))).hexdigest()
    def chk_etag(self):
        self.set_etag_header()
        if self.check_etag_header():
            self.set_status(304)
            return True
        else:
            return False

    def render(self, *args, **kwargs):
        kwargs['staticFile'] = self.staticFile
        kwargs['prefix'] = self.serverPrefix
        kwargs['protocol'] = self.protocol
        kwargs['host'] = self.host
        kwargs['urlprefix'] = self.urlprefix
        kwargs['dap_requires_key'] = self.dap_requires_key
        return super(RequestHandler, self).render(*args, **kwargs)
    def deny(self):
        self.set_status(403)
