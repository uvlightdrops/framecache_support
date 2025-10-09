import logging
import json
#from logging.config import fileConfig
#from os.path import exists, dirname
#from os import makedirs
from anytree import AnyNode, Node, RenderTree
from .AbstractBase import AbstractReader
from anytree.importer import JsonImporter
from flowpy.utils import setup_logger
#from anytreeStorage import AnytreeStorage, CustomNode, Entry

logfn = __name__+'.log'
logger = setup_logger(__name__, logfn, level=logging.DEBUG)


class JsonReader(AbstractReader):
    """ access a set of files as input """
    cfg_si = {}
    reader = {}

    def init_reader(self):
        logger.debug('opening file %s', self.fn_in)
        pass

    def get_fieldnames(self, fn=None):
        """ read first sheet and return col names """
        if not fn: fn = self.cfg_si['out_fns'][0]
        self.fieldnames = list(self.buffer[fn])
        return self.fieldnames

    def read(self, fn):
        file_path = data_in.joinpath(fn + '.yml')
        with open(file_path) as file:
            self.buffer[fn] = json.load(file)

    def read_all(self):
        for fn in self.cfg_si['out_fns']:
            self.read(fn)

    def get_buffer(self, fn):
        return self.buffer[fn]



