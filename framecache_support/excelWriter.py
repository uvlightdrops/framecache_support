import logging
from email.policy import default

import pandas as pd
from flowpy.utils import setup_logger, safe_fn
#from fileBase import FileBase
from .baseWriter import BaseWriter

logger = setup_logger(__name__, __name__+'.log')


class ExcelWriter(BaseWriter): #, FileBase):
    """ handles a set of destination files, writes to excel"""
    writer = None

    # XXX wording: file location is not out_fn.
    def __init__(self):
        #self.writer = {}  # XXX where is this from??
        self.buffer = {}
        self.out_fns = []

    def set_dst(self, dst):
        self.dst = dst.with_suffix('.xlsx')
        #logger.debug("setting dst for writer %s", self.name ) # self.dst)

    def init_writer_all(self):
        # for Excel there is only one dest file
        self.writer = pd.ExcelWriter(self.dst, engine='xlsxwriter')

    def write(self):
        logger.debug("writing %s - self.out_fns : %s", self.name, self.out_fns)
        default_height = 14
        for out_fn in self.out_fns:
            sheet_name = safe_fn(out_fn)
            logger.debug("len buffer[out_fn] : %s", len(self.buffer[out_fn]))
            #logger.debug("buffer[out_fn] : %s", self.buffer[out_fn])
            #logger.debug("write excel sheet %s in file %s", sheet_name, self.dst)
            # logger.debug('buffer[%s].head() : %s', out_fn, self.buffer[out_fn].head())
            try:
                self.buffer[out_fn].to_excel(self.writer, sheet_name=sheet_name, index=False)
                # logger.debug("wrote fields to %s ", sheet_name)
            except Exception as e:
                logger.error("could not write to %s : %s", sheet_name, str(e))
            # logger.debug('empty? : %s', self.buffer[out_fn].empty)
            if self.buffer[out_fn].empty:
                logger.error("buffer is empty for %s", out_fn)
                continue
            # logger.debug('len(buffer[%s]) : %s', out_fn, len(self.buffer[out_fn]))
            # logger.debug(self.buffer[out_fn]['title_old'].head())
            for column in self.buffer[out_fn]:
                if column == 'title_old':
                    column_length = 30
                else:
                    ilst = self.buffer[out_fn][column].astype(str).map(len)
                    # logger.debug('ilst: %s', ilst)
                    lst = ilst.max()
                    lc = len(column)
                    # logger.debug('lst t: %s, lc t: %s', type(lst), type(lc))
                    # logger.debug('lst: %s, lc: %s', lst, lc)
                    column_length = min(32, max(lst, lc))
                # logger.debug('col: %s column_length: %s', column, type(column_length))
                col_idx = self.buffer[out_fn].columns.get_loc(column)
                #column_length = 20
                self.writer.sheets[sheet_name].set_column(col_idx, col_idx, column_length+3)
                # self.writer.sheets[sheet_name].row_dimensions[row[0].row].height = default_height
            for row_idx in range( len(self.buffer[out_fn])+1 ):
                self.writer.sheets[sheet_name].set_row(row_idx, default_height)

        self.writer.close()
        #self.writer.save()

    """
    def write_orig(self):
        with pd.ExcelWriter(dstfn, engine='xlsxwriter') as writer:
            for out_fn in self.cfg_si['out_fns']:
                logger.info("write excel sheet %s", sheet_name)
                c = 0
                sheet_name = out_fn
                self.buffer[c].to_excel(writer, sheet_name=sheet_name, index=False)
                # logger.debug("wrote fields to %s : %s", sheet_name, self.fnames[c])
                for column in self.buffer[c]:
                    column_length = max(self.buffer[c][column].astype(str).map(len).max(), len(column))
                    logger.debug("max is %s", column_length)
                    col_idx = self.buffer[c].columns.get_loc(column)
                    writer.sheets[sheet_name].set_column(col_idx, col_idx, column_length)
                c += 1
            writer.save()
    """
