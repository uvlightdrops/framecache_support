from importlib.resources import as_file

import polars as pl
import pandas as pd
from .baseReader import BaseReader
from .sqlTableMixin import SQLTableMixin
from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')


class DbReader(BaseReader, SQLTableMixin):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.conn = None
        self.query = None
        self.params = None
        self.df = None

    def set_src_dir(self, src_dir):
        self.src_dir = src_dir

    def set_outfiles(self, outfns): # XXX
        self.outfns = outfns
        logger.debug('set_outfiles: %s', self.outfns)
        self.fn = outfns[0]  # default to first

    def get_fieldnames(self, fn):
        pass

    def read_all(self):
        for fn in self.outfns:  # or infs ?
            self.read(fn)

    def read(self, fn=None):
        if fn is None:
            fn = self.fn
        fpath = self.src_dir.joinpath(fn)

        if self.df is None:
            polars_df = pl.read_csv(fpath)
            self.df = polars_df.to_pandas()
            logger.debug('%s read into dbReader df (pandas)', self.fn)
    """
    # SQL-Query jetzt über Mixin
    def read_sql(self, key, value):
        self.read(self.fn)
        query = f"SELECT * FROM df WHERE {key}='{value}'"
        result_df = super().read_sql(query, self.df)
        self.result = result_df.to_dict(orient='records')
        return self.result
    """
