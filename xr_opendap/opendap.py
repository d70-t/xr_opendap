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
import xdrlib
import itertools
import re

indentFill = '    '

openDapTypes = {'uint8':   'Byte',
                'int16':   'Int16',
                'uint16':  'UInt16',
                'int32':   'Int32',
                'uint32':  'UInt32',
                'float32': 'Float32',
                'float64': 'Float64'}

def dtype2dapTypecode(data):
    size = data.dtype.itemsize
    if size > 1 and size < 4:
        size = 4
    return '>%s%d'%(data.dtype.kind, size)

def renderDASAttribute(name, content):
    if isinstance(content, unicode):
        content = content.encode('utf-8')
    return "string %s \"%s\""%(name, str(content).replace('\\', '\\\\').replace('"','\\"'))

class DAPDataType(object):
    def _repr_das_(self):
        return ''.join(self._repr_das_iter_())
    def _repr_dds_(self):
        return ''.join(self._repr_dds_iter_())

class Array(DAPDataType):
    def __init__(self, name, dimensions, dtype, data=None, projections=None, description=None):
        self.name = name
        self.dimensionNames, self.dimensionSize = zip(*dimensions)
        self.dtype = dtype
        self.data = data
        self.description = description
        if projections is None:
            self.size = self.dimensionSize
            self.projection = None
        else:
            if len(projections) != 1:
                raise ValueError('can only handle one projection')
            self.projection = projections[0]
            try:
                self.size = self.projection.size + self.dimensionSize[len(self.projection.size):]
            except AttributeError:
                self.size = self.dimensionSize
    def _repr_das_iter_(self, indent=0):
        yield "%s%s {\r\n"%(indentFill*indent, self.name)
        for name, content in self.description.items():
            if name[0] == '_':
                continue
            yield "%s%s;\r\n"%(indentFill*(indent+1), renderDASAttribute(name, content))
        yield "%s}\r\n"%(indentFill*indent)
    def _repr_dds_iter_(self, indent=0):
        dims = ''.join('[%s=%d]'%(name,size) for name,size in zip(self.dimensionNames, self.size))
        yield "%s%s %s%s;\r\n"%(indentFill*indent, openDapTypes[str(self.dtype)], self.name, dims)
    def generateData(self, data=None):
        if data is None:
            if self.data is None:
                raise ValueError('no data given')
            data = self.data
        shape = self.size
        print shape
        size = np.prod(shape)
        p = xdrlib.Packer()
        p.pack_int(size)
        p.pack_int(size)
        yield p.get_buffer()
        if len(shape) == 1:
            print self.projection
            if self.projection is not None:
                if isinstance(self.data,np.ndarray):
                    data = self.data[self.projection.numpySlice]
                else:
                    data = self.data.get()[self.projection.numpySlice]
            else:
                if isinstance(self.data,np.ndarray):
                    data = self.data
                else:
                    data = self.data.get()
            yield data.ravel().astype(dtype2dapTypecode(data)).tostring()
        elif any([x == 1 for x in shape]):
            for axis, size in enumerate(shape): # pragma: no branch
                if size == 1:
                    print "found unity sized axis, transposing!"
                    pos = self.projection.numpySlice[axis].start
                    data = iter(data.T(axis).S(pos, pos+1)).next()[np.newaxis,...]
                    data = np.rollaxis(data, 0, axis+1)
                    newSlice = self.projection.numpySlice[:axis] + (slice(0,1),) + self.projection.numpySlice[axis+1:]
                    yield data[newSlice].ravel().astype(dtype2dapTypecode(data)).tostring()
                    break
        else:
            if self.projection is not None and len(self.projection.numpySlice) > 0:
                iterSlice = self.projection.numpySlice[0]
                partSlice = self.projection.numpySlice[1:]
                i = 0
                step = iterSlice.step
                if step is None:
                    step = 1
                for subarray in data.S(iterSlice.start, iterSlice.stop):
                    i += 1
                    if i >= step:
                        i = 0
                    else:
                        continue
                    yield subarray[partSlice].ravel().astype(dtype2dapTypecode(data)).tostring()
            else:
                for subarray in data:
                    yield subarray.ravel().astype(dtype2dapTypecode(data)).tostring()

class Sequence(DAPDataType):
    def __init__(self, name, components, data=None):
        self.name = name
        self.components = components
        self.data = None
    def _repr_das_iter_(self, indent=0):
        yield "%s%s {\r\n"%(indentFill*indent, self.name)
        for component in self.components:
            for line in component._repr_das_iter_(indent+1):
                yield line
        yield "%s}\r\n"%(indentFill*indent)
    def _repr_dds_iter_(self, indent=0):
        yield "%ssequence {\r\n"%(indentFill*indent,)
        for component in self.components:
            for line in component._repr_dds_iter_(indent+1):
                yield line
        yield "%s} %s;\r\n"%(indentFill*indent, self.name)
        

class Structure(Sequence):
    def _repr_dds_iter_(self, indent=0):
        yield "%sstructure {\r\n"%(indentFill*indent,)
        for component in self.components:
            for line in component._repr_dds_iter_(indent+1):
                yield line
        yield "%s} %s;\r\n"%(indentFill*indent, self.name)
    def generateData(self, data=None):
        if data is None:
            data = self.data
        if data is None:
            for c in self.components:
                for d in c.generateData():
                    yield d
        else:
            for c,d in itertools.izip(self.components, data):
                for d2 in c.generateData(d):
                    yield d2

class Dataset(Structure):
    def __init__(self, components, id, source='', history='', references='', date=None):
        super(Dataset,self).__init__('attributes', components)
        self.id = id
        self.attributes = {
            'Conventions': 'CF-1.6',
            'version': 'pre-Alpha',
            'sensor': 'specMACS',
            'title': 'Munich Aerosol Cloud Scanner data',
            'institution': 'Meterorological Institute Munich',
            'source': source,
            'history': history,
            'references': references}
        if date is not None:
            self.attributes['date'] = date2seconds(date)
            self.attributes['isodate'] = date.isoformat()
    def _repr_das_iter_(self, indent=0):
        yield "%sattributes {\r\n"%(indentFill*indent,)
        for name, content in self.attributes.items():
            if name[0] == '_':
                continue
            yield "%s%s;\r\n"%(indentFill*(indent+1), renderDASAttribute(name, content))
        for component in self.components:
            for line in component._repr_das_iter_(indent+1):
                yield line
        yield "%s}\r\n"%(indentFill*indent,)
    def _repr_dds_iter_(self, indent=0):
        yield "%sdataset {\r\n"%(indentFill*indent,)
        for component in self.components:
            for line in component._repr_dds_iter_(indent+1):
                yield line
        yield "%s} %s;\r\n"%(indentFill*indent, self.id)

def product2Dataset(product, projections=None, references=""):
    components = []
    if projections is not None and len(projections) == 0:
        projections = None
    for name, dimension in product.productDimensions.items():
        if name in HIDDEN_PARTS:
            continue
        if projections is not None:
            applicableProjections = []
            for p in projections:
                if name in p.requestedParts:
                    applicableProjections.append(p)
            if len(applicableProjections) == 0:
                continue
        else:
            applicableProjections = None
        part = product[name]
        description = product.partDescription[name]
        if '_transform' in description:
            data = description['_transform'](part.get())
        else:
            data = part
        if str(data.dtype) not in openDapTypes:
            continue
        components.append(Array(name, zip(dimension, data.shape), data.dtype, data, applicableProjections, description=description))
        #components.append(Array(name, zip(dimension, data.shape), 'float32', data, applicableProjections))
    try:
        source='Hyperspectral Imager %s'%product.sensorId
    except AttributeError:
        source='specMACS measurement system'
    return Dataset(components,
                   product.hash,
                   source=source,
                   history='\r\n'.join(product.getPrintableComponentHistory()),
                   references=references,
                   date=getattr(product, 'date', None))

class Projection(object):
    idProjectionRe = re.compile(r"(?P<id>[a-zA-Z_%\.][a-zA-Z0-9/_%\.]*)")
    arrayProjectionRe = re.compile(r"(?P<id>[a-zA-Z_%\.][a-zA-Z0-9/_%\.]*)(?P<dim>(?:\[[0-9]+(?::[0-9]+){0,2}\])+)")
    @classmethod
    def parse(cls,projectionString):
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

class OpenDAPHandler(RequestHandler):
    def initialize(self, *args, **kwargs):
        super(OpenDAPHandler, self).initialize(*args, **kwargs)
        self.set_header('XDODS-Server', 'dods/3.2.2')
        self.set_header('Expires', datetime.datetime.now() + opendapPageExpiryTime)
        self.set_header('Cache-Control', 'max-age=%d'%int(opendapPageExpiryTime.total_seconds()))
        self.set_header('Vary', 'X-Auth-Roles')
    def _references(self,objectId):
        return '%s%s/info/%s.html'%(self.urlprefix, self.serverPrefix, objectId)

class DASHandler(OpenDAPHandler):
    def initialize(self, *args, **kwargs):
        super(DASHandler, self).initialize(*args, **kwargs)
        self.set_header('Content-Description', 'dods-das')
        self.set_header('Content-Type', 'text/plain')
        
    def get(self,objectId):
        if self.chk_etag():
            return
        product = self.application.productaccessor.get(objectId)
        self.write(product2Dataset(product,references=self._references(objectId))._repr_das_())

class DDSHandler(OpenDAPHandler):
    def initialize(self, *args, **kwargs):
        super(DDSHandler, self).initialize(*args, **kwargs)
        self.set_header('Content-Description', 'dods-dds')
        self.set_header('Content-Type', 'text/plain')
        
    def get(self,objectId):
        if self.chk_etag():
            return
        product = self.application.productaccessor.get(objectId)
        self.write(product2Dataset(product,references=self._references(objectId))._repr_dds_())

class DataDDSHandler(OpenDAPHandler):
    def initialize(self, *args, **kwargs):
        super(DataDDSHandler, self).initialize(*args, **kwargs)
        self.set_header('Content-Description', 'dods-data')
        self.set_header('Content-Type', 'application/octet')
        #self.set_header('Content-Type', 'text/plain')

    @gen.coroutine
    def get(self,objectId):
        if self.chk_etag():
            return
        projItems = filter(lambda x: x is not None, map(Projection.parse, self.request.query.split(',')))
        product = self.application.productaccessor.get(objectId)
        dataset = product2Dataset(product, projItems,references=self._references(objectId))
        self.write(dataset._repr_dds_())
        self.write("\nData:\n")
        for d in dataset.generateData():
            self.write(d)
            yield self.flush()
        self.finish()

