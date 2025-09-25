from abc import ABC, abstractmethod
from pykeepass.baseelement import BaseElement

class AbstractReader(ABC):
    in_SI = None

    @abstractmethod
    def read(self, fn):
        pass

    @abstractmethod
    def get_fieldnames(self, fn):
        """ read sheet named fn and return col names """
        pass

    @abstractmethod
    def set_src_dir(self, src_dir):
        """ the 'base' filepath of the reader, ie dir of files to read """
        pass

class AbstractWriter(ABC):
    @abstractmethod
    def write(self):
        pass

    @abstractmethod
    def init_writer(self):
        pass

    @abstractmethod
    def set_dst(self, dstfn):
        pass

    @abstractmethod
    def set_outfiles(self, out_fns):
        """ set the names of the output files or sheets """
        pass

    @abstractmethod
    def set_buffer(self, fn, buffer):
        """ Abstract method for setting the buffer."""
        pass


class AbstractStorage(ABC):

    def _find_group(self, recursive=True, path=None, group=None, **kwargs):
        pass

    @abstractmethod
    def _find_group_by_path(self, path: list or str) -> BaseElement:
        pass

    def _find_group_by_path_slashed(self, path_slashed: str) -> BaseElement:
        pass

    def _find_group_by_name(self, name: str) -> BaseElement:
        """ find group by name """
        pass

    @abstractmethod
    def _find_entry_by_path(self, path):
        pass

    # not useful up to now
    def load_hierarchy(self, path):
        pass
