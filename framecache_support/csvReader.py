import logging
from .baseReader import BaseReader
import pandas as pd
from flowpy.utils import setup_logger

logfn = __name__+'.log'
logger = setup_logger(__name__, logfn)
DELIM_IN = ','


class CsvReader(BaseReader):
    """ access a set of files as input """
    cfg_si = {}
    reader = {}

    def __init__(self):
        self.buffer = {}

    def init_reader(self):
        logger.debug(self.cfg_si.keys())
        logger.debug('opening file %s', self.cfg_si['in_fns'])
        pass

    def get_fieldnames(self, fn=None):
        """ read first sheet and return col names """
        if fn is None:
            fn = self.cfg_si['out_fns'][0]
        self.fieldnames = list(self.buffer[fn])
        return self.fieldnames

    def read(self, fn):
        file_path = self.src_dir.joinpath(fn+'.csv')
        self.buffer[fn] = pd.read_csv(file_path)

    def read_all(self):
        for fn in self.cfg_si['out_fns']:
            self.read(fn)

    def read_first(self):
        fn = self.cfg_si['out_fns'][0]
        self.read(fn)
