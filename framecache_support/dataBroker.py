from yaml_config_support.YamlConfigSupport import YamlConfigSupport
from utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')

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


class APIbroker:
    pass

#class DataBroker(YamlConfigSupport):
class DataBroker:

    def class_factory(self, class_name, rw):
        if rw == 'r':
            class_name = class_name+'Reader'
        elif rw == 'w':
            class_name = class_name+'Writer'
        elif rw == 's':
            class_name = class_name+'Storage'
        else:
            pass
        # define mapping in config XXX
        classes = {
            #'SICache': SICache,
            #'MetadataSearch': MetadataSearch,
            'excelReader': ExcelReader,
            'csvReader': CsvReader,
            'excelWriter': ExcelWriter,
            'csvWriter': CsvWriter,
            'dbWriter': DbWriter,
            #'ansibleWriter': AnsibleWriter,
            'yamlReader': YamlReader,
            'yamlWriter': YamlWriter,
            #'yamlStorage': YamlStorage,
            #'yamldirStorage': YamldirStorage,
            'jsonReader': JsonReader,
            'jsonWriter': JsonWriter,
            #'jsonStorage': JsonStorage,
            #'firefox_bookmarksStorage': FirefoxBookmarksStorage,
            #'kdbxStorage': KdbxStorage,
        }
        return classes[class_name]()

    # XXX combine both, usage check!
    # XXX add klass_cfg as param ? And use default from config as fallback
    def init_reader_class(self):
        klass_cfg = self.cfg_profile['reader']
        return self.class_factory(klass_cfg, 'r')

    def init_writer_class(self):
        klass_cfg = self.cfg_profile['writer']
        return self.class_factory(klass_cfg, 'w')

    def init_storage_src_class(self):
        klass_cfg = self.cfg_profile['storage_src']
        klass_obj = self.class_factory(klass_cfg, 's')
        klass_obj.src_or_dst = 'src'
        return klass_obj

    def init_storage_dst_class(self):
        klass_cfg = self.cfg_profile['storage_dst']
        klass_obj = self.class_factory(klass_cfg, 's')
        klass_obj.src_or_dst = 'dst'
        return klass_obj


# dont make dependency to DataBroker
# we use the class as a member of the main logic class tree
