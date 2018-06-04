# -*- coding: utf-8 -*-

if __name__ == '__main__':
    import matplotlib as mpl
    mpl.use('Agg')

import os
import subprocess
import tornado.web
import tornado.ioloop
#from raven.contrib.tornado import AsyncSentryClient
#from raven.contrib.tornado import SentryMixin
#from raven.handlers.logging import SentryHandler

from .opendap import DASHandler, DDSHandler, DataDDSHandler

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/dap/(?P<objectId>[0-9a-f]+)\.das", DASHandler),
            (r"/dap/(?P<objectId>[0-9a-f]+)\.dds", DDSHandler),
            (r"/dap/(?P<objectId>[0-9a-f]+)\.dods", DataDDSHandler),
            ]
        settings = dict(
            template_path=os.path.join(os.path.dirname(__file__), "templates"),
            static_path=os.path.join(os.path.dirname(__file__), "static"),
            debug=True,
            )
        tornado.web.Application.__init__(self, handlers, **settings)

if __name__ == '__main__':
    import tornado.options
    from tornado.options import define, options
    define("port", default=8887, help="run on the given port", type=int)
    define("mock", default=False, type=bool, help="Should we use a fully mocked database?")
    tornado.options.parse_command_line()

    application = Application()
    application.settings['git-rev'] = subprocess.Popen(['git', 'rev-parse', 'HEAD'], stdout=subprocess.PIPE).communicate()[0].strip()

    application.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()
