# -*- coding: utf-8 -*-

#if __name__ == '__main__':
#    import matplotlib as mpl
#    mpl.use('Agg')

import os
import datetime
import subprocess
import tornado.web
import tornado.ioloop
import yaml

from xr_opendap.opendap import DASHandler, DDSHandler, DataDDSHandler
from xr_opendap.datalocator import parse_location_config

class Application(tornado.web.Application):
    def __init__(self, config):
        handlers = [
            (r"/(?P<objectId>.+)\.das$", DASHandler),
            (r"/(?P<objectId>.+)\.dds$", DDSHandler),
            (r"/(?P<objectId>.+)\.dods$", DataDDSHandler),
            ]
        settings = dict(
            template_path=os.path.join(os.path.dirname(__file__), "templates"),
            static_path=os.path.join(os.path.dirname(__file__), "static"),
            debug=True,
            opendapPageExpiryTime=datetime.timedelta(seconds=5),
            data_locator=parse_location_config(config["sources"]),
            )
        tornado.web.Application.__init__(self, handlers, **settings)

if __name__ == '__main__':
    import tornado.options
    from tornado.options import define, options
    define("port", default=8887, help="run on the given port", type=int)
    define("config", default="config.yaml", type=str, help="path to config file")
    tornado.options.parse_command_line()

    application = Application(yaml.load(open(options.config)))
    application.settings['git-rev'] = subprocess.Popen(['git', 'rev-parse', 'HEAD'], stdout=subprocess.PIPE).communicate()[0].strip()

    application.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()
