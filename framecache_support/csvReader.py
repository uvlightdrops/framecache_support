import logging
from .baseReader import BaseReader
from .sqlTableMixin import SQLTableMixin
import pandas as pd
from flowpy.utils import setup_logger

logfn = __name__+'.log'
logger = setup_logger(__name__, logfn)
DELIM_IN = ','


class CsvReader(BaseReader, SQLTableMixin):
    """ Access a set of files as input with optional SQL support """
    cfg_si = {}
    reader = {}

    def __init__(self):
        super().__init__()
        self.buffer = {}
        self.df = None  # Main DataFrame for SQL operations

    def init_reader(self):
        logger.debug(self.cfg_si.keys())
        logger.debug('opening file %s', self.cfg_si['in_fns'])


    def get_fieldnames(self, fn=None):
        """ read first sheet and return col names """
        if fn is None:
            fn = self.cfg_si['out_fns'][0]
        self.fieldnames = list(self.buffer[fn])
        return self.fieldnames

    def read(self, fn):
        file_path = self.src_dir.joinpath(fn+'.csv')
        self.buffer[fn] = pd.read_csv(file_path)
        self.df = self.buffer[fn]  # Set main DataFrame for SQL operations

    def read_all(self):
        for fn in self.cfg_si['out_fns']:
            self.read(fn)

    def read_first(self):
        fn = self.cfg_si['out_fns'][0]
        self.read(fn)

    def read_sql(self, key, value):
        """ Perform SQL query on the main DataFrame """
        if self.df is None:
            raise ValueError('No DataFrame loaded for SQL operations.')
        query = f"SELECT * FROM df WHERE {key}='{value}'"
        result_df = super().read_sql(query, self.df)
        return result_df
