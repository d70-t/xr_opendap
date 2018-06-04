# -*- coding: utf-8 -*-
# Filename: opendap.py
"""
frontendServer - render macsServer database to web page
=======================================================

Copyright (C) 2013 Tobias Kölling
"""

from .tools import RequestHandler
from tornado import gen
import tornado.web
import numpy as np
import xarray as xr
import xdrlib
import itertools
import re
import datetime
import urllib

indentFill = '    '

openDapTypes = {'uint8':   'Byte',
                'int8':    'Byte',
                'int16':   'Int16',
                'uint16':  'UInt16',
                'int32':   'Int32',
                'uint32':  'UInt32',
                'float32': 'Float32',
                'float64': 'Float64',
                'datetime64[ns]': 'Float64',
                'int64': 'Float64',
               }

openDapFormat = {'Byte':   'd',
                 'Int16':   'd',
                 'UInt16':  'd',
                 'Int32':   'd',
                 'UInt32':  'd',
                 'Float32': 'g',
                 'Float64': 'g',
                 }

openDapNumpyCodes = {'Byte':   '>u1',
                     'Int16':   '>i2',
                     'UInt16':  '>u2',
                     'Int32':   '>i4',
                     'UInt32':  '>u4',
                     'Float32': '>f4',
                     'Float64': '>f8',
                    }

def dtype2dapTypecode(data):
    daptype = openDapTypes[str(data.dtype)]
    return openDapNumpyCodes[daptype]

def renderDASAttribute(name, content):
    try:
        shape = content.shape
    except AttributeError:
        return u"string %s \"%s\""%(name, content.replace('\\', '\\\\').replace('"','\\"'))

    assert(len(shape) <= 2)
    daptype = openDapTypes[str(content.dtype)]
    dapformat = openDapFormat[daptype]
    fmt = u"{:" + dapformat + u"}"
    if len(shape) == 0:
        return (u"{:s} {:s} " + fmt).format(daptype, name, content)
    else:
        return u"{:s} {:s}".format(daptype, name) + u"{" +\
               u", ".join(fmt.format(c) for c in content) + \
               u"}"

def dsiter(ds):
    for name, coord in ds.coords.items():
        yield name, coord

class Projection(object):
    idProjectionRe = re.compile(r"(?P<id>[a-zA-Z_%\.][a-zA-Z0-9/_%\.]*)")
    arrayProjectionRe = re.compile(r"(?P<id>[a-zA-Z_%\.][a-zA-Z0-9/_%\.]*)(?P<dim>(?:\[[0-9]+(?::[0-9]+){0,2}\])+)")
    @classmethod
    def parse(cls,projectionString):
        projectionString = urllib.unquote(projectionString)
        m = cls.arrayProjectionRe.match(projectionString)
        if m is not None:
            return ArrayProjection(**m.groupdict())
        m = cls.idProjectionRe.match(projectionString)
        if m is not None:
            return IdProjection(**m.groupdict())

class IdProjection(Projection):
    def __init__(self, id):
        self.id = id
        self.requestedParts = [id]
    @property
    def numpySlice(self):
        return tuple()
    def __repr__(self):
        return "IdProjection({})".format(self.id)

class ArrayProjection(Projection):
    def __init__(self, id, dim):
        self.id = id
        self.requestedParts = [id]
        self.hyperslabs = []
        for part in dim[1:-1].split(']['):
            elements = map(int,part.split(':'))
            if len(elements) == 1:
                self.hyperslabs.append((elements[0],1,elements[0]))
            elif len(elements) == 2:
                self.hyperslabs.append((elements[0],1,elements[1]))
            elif len(elements) == 3: # pragma no branch
                self.hyperslabs.append(tuple(elements))
    @property
    def size(self):
        return tuple(int(np.ceil(float(end+1-start)/stride)) for start, stride, end in self.hyperslabs)
    @property
    def numpySlice(self):
        return tuple(slice(start, end+1, stride) for start, stride, end in self.hyperslabs)
    def __repr__(self):
        return "ArrayProjection(%s,%s)"%(self.id, ''.join(["[%d:%d:%d]"%h for h in self.hyperslabs]))

def xr2das(ds, indent=0, name="attributes"):
    yield u"%s%s {\r\n"%(indentFill*indent,name)
    for name, content in ds.attrs.items():
        attr = renderDASAttribute(name, content)
        if attr is not None:
            yield u"%s%s;\r\n"%(indentFill*(indent+1), attr)
    try:
        items = ds.items()
    except AttributeError:
        pass
    else:
        for name, component in items:
            for line in xr2das(component, indent+1, name):
                yield line
    yield u"%s}\r\n"%(indentFill*indent,)

def xr2dds(ds, name="dataset", indent=0):
    if isinstance(ds, xr.Dataset):
        kind = "dataset"
    elif isinstance(ds, list):
        kind = "dataset"
    elif isinstance(ds, xr.DataArray):
        kind = "array"
    if kind == "array":
        daptype = openDapTypes[str(ds.dtype)]
        yield u"%s%s %s"%(indentFill*indent, daptype, name) + \
              u"".join(u"[{}={}]".format(k, v) for k, v in ds.sizes.items()) + \
              u";\r\n"
        return
    yield "%s%s {\r\n"%(indentFill*indent, kind)
    for cname, component in ds:
        for line in xr2dds(component, cname, indent+1):
            yield line
    yield "%s} %s;\r\n"%(indentFill*indent, name)

def xrda2xdr(da):
    size = np.prod(da.shape)
    typecode = dtype2dapTypecode(da)
    print "converting {} -> {}".format(da.dtype, typecode)
    print da.shape
    p = xdrlib.Packer()
    p.pack_int(size)
    p.pack_int(size)
    yield p.get_buffer()
    if len(da.shape) > 1 and np.prod(da.shape[1:]) > 1024:
        for subarray in da:
            yield subarray.load().data.ravel().astype(typecode).tostring()
    else:
        yield da.load().data.ravel().astype(typecode).tostring()



class OpenDAPHandler(RequestHandler):
    def initialize(self, *args, **kwargs):
        super(OpenDAPHandler, self).initialize(*args, **kwargs)
        self.set_header('XDODS-Server', 'dods/3.2.2')
        self.set_header('Expires', datetime.datetime.now() + self.settings["opendapPageExpiryTime"])
        self.set_header('Cache-Control', 'max-age=%d'%int(self.settings["opendapPageExpiryTime"].total_seconds()))
        self.set_header('Vary', 'X-Auth-Roles')
    def _references(self,objectId):
        return '%s%s/info/%s.html'%(self.urlprefix, self.serverPrefix, objectId)
    def locate(self, objectId):
        return self.settings["data_locator"].locate(objectId)

class DASHandler(OpenDAPHandler):
    def initialize(self, *args, **kwargs):
        super(DASHandler, self).initialize(*args, **kwargs)
        self.set_header('Content-Description', 'dods-das')
        self.set_header('Content-Type', 'text/plain; charset=utf-8')
        
    def get(self,objectId):
        if self.chk_etag():
            return
        data = self.locate(objectId)
        self.write(u"".join(xr2das(data)).encode("utf-8"))

class DDSHandler(OpenDAPHandler):
    def initialize(self, *args, **kwargs):
        super(DDSHandler, self).initialize(*args, **kwargs)
        self.set_header('Content-Description', 'dods-dds')
        self.set_header('Content-Type', 'text/plain; charset=utf-8')
        
    def get(self,objectId):
        if self.chk_etag():
            return
        name = objectId.split("/")[-1]
        data = self.locate(objectId)
        projItems = filter(lambda x: x is not None, map(Projection.parse, self.request.query.split(',')))
        data = self.locate(objectId)
        if len(projItems) > 0:
            values = [(p.id, data[p.id][p.numpySlice]) for p in projItems]
        else:
            values = list(data.items())
        self.write(u"".join(xr2dds(values, name)))

class DataDDSHandler(OpenDAPHandler):
    def initialize(self, *args, **kwargs):
        super(DataDDSHandler, self).initialize(*args, **kwargs)
        self.set_header('Content-Description', 'dods-data')
        self.set_header('Content-Type', 'application/octet')
        #self.set_header('Content-Type', 'text/plain')

    @gen.coroutine
    def get(self, objectId):
        if self.chk_etag():
            return
        projItems = filter(lambda x: x is not None, map(Projection.parse, self.request.query.split(',')))
        name = objectId.split("/")[-1]
        data = self.locate(objectId)
        print list(projItems)
        if len(projItems) > 0:
            values = [(p.id, data[p.id][p.numpySlice]) for p in projItems]
        else:
            values = list(data.items())
        self.write(u"".join(xr2dds(values, name)))
        self.write("\nData:\n")
        for cname, component in values:
            for part in xrda2xdr(component):
                self.write(part)
                yield self.flush()
        self.finish()
