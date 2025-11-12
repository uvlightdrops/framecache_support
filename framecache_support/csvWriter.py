#!/usr/bin/python3
from flowpy.utils import setup_logger
from .baseWriter import BaseWriter

logfn = __name__+'.log'
logger = setup_logger(__name__, logfn)


class CsvWriter(BaseWriter): #, ConfigLoader):
    def __init__(self):
        self.writer = {}
        self.buffer = {}
        self.fn_out_f = {}

    def set_dst(self, dst):
        """ for csv writer, dst is a directory """
        dst_dir = dst
        self.set_dst_dir(dst_dir)

    def set_dst_dir(self, dst_dir):
        logger.debug('setting dst_dir to %s', dst_dir)
        self.dst_dir = dst_dir

    def init_writer_all(self):
        c = 0
        for out_fn in self.out_fns:
            self.init_writer(out_fn, c)
            c += 1

    def init_writer(self, fn, c):
        """ prepare dict of csv writers for all dest files """
        self.fn_out_f[c] = self.dst_dir.joinpath(fn).with_suffix('.csv')
        logger.debug('fn_out_f: %s', self.fn_out_f[c])

    def write(self):
        c = 0
        # check that dst_dir exists
        if not self.dst_dir.exists():
            self.dst_dir.mkdir()
        for fn in self.out_fns:
            logger.debug('type of df is %s', type(self.buffer[fn]))
            # logger.debug('self.buffer[fn]: %s', self.buffer[fn][:1])
            self.buffer[fn].to_csv(self.dst_dir.joinpath(fn).with_suffix('.csv'), index=False)
            c += 1

