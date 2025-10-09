from importlib.resources import as_file

import polars as pl
from .baseReader import BaseReader
from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')


class DbReader(BaseReader):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.conn = None
        self.query = None
        self.params = None
        self.df = None

    def set_src_dir(self, src_dir):
        self.src_dir = src_dir

    def get_fieldnames(self, fn):
        pass

    def read_all(self):
        for fn in self.src_dir.joinpath(fn+'.csv'):
            self.read(fn)

    def read(self, fn):
        fpath = self.src_dir.joinpath(fn)
        if self.df is None:
            self.df = pl.read_csv(fpath)
            logger.debug('read into dbReader df')
        #self.read_sql()

    # XXX all methods not finished
    def read_sql(self, key, value):
        fn = 'hosts.csv'
        self.read(fn)
        #logger.debug('read_sql, df shape: %s', df.shape)
        # SQL-Abfrage
        #sql = "SELECT * FROM df WHERE {key}='{value}'", %(key, value)
        sql = "SELECT * FROM df WHERE {}='{}'".format(key, value)
        logger.debug(sql)
        args = {'df': self.df}
        ctx = pl.SQLContext(args) #, eager=True)
        tmp = ctx.execute(sql)
        poldf = tmp.collect()
        #logger.debug('poldf %s --- %s', type(poldf), poldf)
        self.result = poldf.to_dict(as_series=False)
        #logger.debug('result : %s', self.result)
        return self.result
