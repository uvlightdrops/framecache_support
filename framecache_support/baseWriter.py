from .AbstractBase import AbstractWriter
from flowpy.utils import setup_logger
import logging
logfn = __name__+'.log'
logger = setup_logger(__name__, logfn, level=logging.DEBUG)
from pathlib import Path


class BaseWriter(AbstractWriter):
    def set_buffer(self, fn, buffer):
        self.buffer[fn] = buffer

    def set_name(self, name):
        self.name = name

    def set_dst(self, dst: Path):
        self.set_dst_fn(dst)

    def set_outfiles(self, out_fns):
        self.out_fns = out_fns

    def init_writer(self, out_fn):
        pass

    def writerow(self, row):
        pass

