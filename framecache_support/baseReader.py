from .AbstractBase import AbstractReader
from pathlib import Path

class BaseReader(AbstractReader):
    def __init__(self, *args, **kwargs):
        self.buffer = {}
    def get_buffer(self, fn):
        return self.buffer[fn]
    def set_src_dir(self, src_dir: Path) -> None:
        self.src_dir = src_dir
    def set_fn(self, fn):
        self.fn = fn
    def set_name(self, name):
        self.name = name
