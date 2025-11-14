# from abc import abstractmethod
import yaml
from .baseReader import BaseReader

from flowpy.utils import setup_logger
logfn = __name__+'.log'
logger = setup_logger(__name__, logfn)

class YamlReader(BaseReader):
    """ access a set of files as input """
    cfg_si = {}
    reader = {}

    def __init__(self, *args, **kwargs):
        super().__init__()

    def init_reader(self):
        logger.debug('opening file %s', self.fn_in)
        pass

    def get_fieldnames(self, fn=None):
        """ read first sheet and return col names """
        if not fn: fn = self.cfg_si['out_fns'][0]
        self.fieldnames = list(self.buffer[fn])
        return self.fieldnames

    def read(self, fn):
        # XXX this method not in use?  - data_in not defined
        file_path = self.cfg_si['data_in_sub'].joinpath(fn + '.yml')
        self.buffer[fn] = yaml.load(file_path, Loader=yaml.FullLoader)

    def read_all(self):
        for fn in self.cfg_si['out_fns']:
            self.read(fn)
