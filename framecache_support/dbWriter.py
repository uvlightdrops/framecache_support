import logging
import pandas as pd
from flowpy.utils import setup_logger, safe_fn
from .baseWriter import BaseWriter
# from config_loader import ConfigLoader
from sqlalchemy import create_engine, inspect

logger = setup_logger(__name__, __name__+'.log')


class DbWriter(BaseWriter) : #, ConfigLoader):
    """ handles a set of destination files, writes to excel"""
    writer = None

    # XXX wording: file location is not out_fn.
    def __init__(self, *args, **kwargs):
        #self.writer = {}  # XXX where is this from??
        self.buffer = {}
        self.out_fns = []
        logger.debug("init writer")

    def set_dst(self, dst):
        # logger.debug("setting dst to %s", dst)
        self.dst = dst

    def init_writer_all(self):
        if self.cfg_si['pw_out']:
            self.pw = self.cfg_si['pw_out']
        
        # for DB there is only one dest file/table
        url = "mysql+pymysql://infra:%s@localhost/%s?charset=utf8mb4" %(self.pw, self.dst)
        # logger.debug(self.pw)
        self.writer = create_engine(url)
        logger.debug("init writer for %s", url)

    def write(self):
        logger.debug("self.out_fns : %s", self.out_fns)

        for out_fn in self.out_fns:
            # logger.debug('empty? : %s', self.buffer[out_fn].empty)
            if self.buffer[out_fn].empty:
                logger.error("buffer is empty for %s", out_fn)
                continue

            sheet_name = safe_fn(out_fn)
            logger.info("insert in table(sheet) %s in DB %s", sheet_name, self.dst)

            conn = self.writer.connect()
            inspector = inspect(conn)
            tn = sheet_name  # 'infra.'+sheet_name
            table_exists = inspector.has_table(tn)
            logger.debug('%s table_exists: %s', tn, table_exists)

            if not table_exists:
                self.buffer[out_fn].to_sql(sheet_name, self.writer, index=False)
                
            try:
                self.buffer[out_fn].to_sql(sheet_name, self.writer, if_exists='replace', index=False)
                logger.debug("wrote fields to %s ", sheet_name)
            except Exception as e:
                logger.error("could not write to %s : %s", sheet_name, str(e))


        #self.writer.save()
