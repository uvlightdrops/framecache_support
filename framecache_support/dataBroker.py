from yaml_config_support.yamlConfigSupport import YamlConfigSupport
from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')

from .dbReader import DbReader
from .csvReader import CsvReader
from .excelReader import ExcelReader
from .jsonReader import JsonReader
from .yamlReader import YamlReader

from .dbWriter import DbWriter
from .csvWriter import CsvWriter
from .excelWriter import ExcelWriter
from .jsonWriter import JsonWriter
from .yamlWriter import YamlWriter
"""
"""
#from SICache import SICache, MetadataSearch


#class DataBroker(YamlConfigSupport):
class DataBroker:

    def class_factory(self, class_name, *args, **kwargs):
        rw = kwargs.get('rw', None)
        if rw == 'r':
            class_name = class_name+'Reader'
        elif rw == 'w':
            class_name = class_name+'Writer'
        else:
            return None
        # define mapping in config XXX
        classes = {
            'dbReader': DbReader,
            'csvReader': CsvReader,
            'excelReader': ExcelReader,
            'jsonReader': JsonReader,
            'yamlReader': YamlReader,
            #'ansibleWriter': AnsibleWriter,
            'csvWriter': CsvWriter,
            'dbWriter': DbWriter,
            'excelWriter': ExcelWriter,
            'jsonWriter': JsonWriter,
            'yamlWriter': YamlWriter,
        }
        return classes[class_name](*args, **kwargs)


    def init_reader_class_by_type(self, type):
        return self.class_factory(type, 'r')
    def init_writer_class_by_type(self, type):
        return self.class_factory(type, 'w')

    # XXX combine both, usage check!
    # XXX add klass_cfg as param ? And use default from config as fallback
    def init_reader_class(self, *args, **kwargs):
        logger.debug("init_reader_class kwargs: %s", kwargs)
        klass_cfg = self.cfg_profile['reader']
        if 'reader_type' in kwargs:
            klass_cfg = kwargs['reader_type']
        return self.class_factory(klass_cfg, *args, rw='r', **kwargs)

    def init_writer_class(self, *args, **kwargs):
        klass_cfg = self.cfg_profile['writer']
        if 'writer_type' in kwargs:
            klass_cfg = kwargs['writer_type']
        return self.class_factory(klass_cfg, *args, rw='w', **kwargs)


# dont make dependency to DataBroker
# we use the class as a member of the main logic class tree
