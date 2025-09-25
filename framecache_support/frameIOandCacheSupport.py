import os.path

from .dataBroker import DataBroker
try:
    import win32com.client
except:
    pass
from time import sleep
from utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')
lg = setup_logger(__name__+'_2', __name__+'_2.log')
import pprint
# import sys

class FrameIOandCacheSupport(DataBroker):

    """ Class to support many dataframes IO and caching

    """

    def __init__(self):
        self.df_d = {}
        self.writer_d = {}
        # self.reader_d = {}
        self.buffer_names_d = {}
        self.tkeys_d = {}
        self.frame_fields = {}

        #print(sys.path)

    def init_dfio_dicts(self, tkeys):
        for tkey in tkeys:
            logger.debug('init_dfio_dicts: tkey: %s', tkey)
            self.df_d[tkey] = {}
            self.writer_d[tkey] = {}
            self.buffer_names_d[tkey] = {}

    # XXX own class for table and df support? Or DataBroker better
    def build_fieldlists(self, cfg):
        for key, value in cfg.items():
            if key.endswith('_table'):
                # logger.debug('build_fieldlists for %s', key)
                field_list = []
                for e, val in cfg[key].items():
                    # logger.debug('e: %s, val: %s', e, val)
                    if e == 'sort':
                        continue
                    if e == 'add':
                        field_list = field_list + val
                    else:
                        field_list += cfg[e]
                #logger.debug('e: %s, field_list: %s', e, field_list)
                self.__setattr__('fn_'+key, field_list) # deprecated XXX
                self.frame_fields[key] = field_list # NEW
        # lg.debug('self.fn* : %s', [attr for attr in dir(self) if attr.startswith('fn_')])
        # logger.debug(self.frame_fields['entries_raw_table'])

    def init_r(self, tkeys):
        # XXX self.reader_d = {}  # we want this too
        self.reader = self.init_reader_class()
        self.reader.set_src_dir(self.cfg_si['data_in_sub'])

    def init_w(self, tkeys):
        # legacy writer single object
        self.writer = self.init_writer_class()
        # current array of writers
        for tkey in tkeys:
            self.writer_d[tkey] = self.init_writer_class()
            self.writer_d[tkey].set_name(tkey)

    def close_excel(self):
        try:
            excel = win32com.client.GetObject(None, "Excel.Application")
            # Close the Excel application
            excel.Quit()
            lg.info("Excel application closed successfully.")
        except Exception as e:
            pass
            #lg.debug('Error: %s', e)
        #sleep(1)

    def prep_writer(self):
        # lg.debug('self.phase_subdir: %s', self.phase_subdir)
        p = self.cfg_si['data_out_sub'].joinpath(self.phase_subdir)
        if not p.exists():
            os.makedirs(self.cfg_si['data_out_sub'].joinpath(self.phase_subdir))

        for tk_i, tk_item in self.tkeys_d.items():
            for tkey in tk_item:
                # XXX os.remove here?
                # lg.debug('prep writer for %s', tkey)
                outfiles = list(self.buffer_names_d[tkey].keys())
                self.writer_d[tkey].set_outfiles(outfiles)
                self.writer_d[tkey].set_dst(self.cfg_si['data_out_sub'].joinpath(self.phase_subdir, '_'+tkey))
                # lg.debug('buffer_names_d[%s].keys(): %s', tkey, self.buffer_names_d[tkey].keys())
                for name in self.buffer_names_d[tkey].keys():
                    lg.debug('set buffer len=%s for %s', len(self.df_d[tkey[name]]), name)
                #    self.writer_d[tkey].set_buffer(name, self.df_d[tkey][name])
                setattr(self.writer_d[tkey], 'cfg_si', self.cfg_si)
                self.writer_d[tkey].init_writer_all()

    def generic_write_xlsx_group(self, xgroupnr: int) -> None:
        for tkey in self.tkeys_d[xgroupnr]:
            lg.info('=== write all for tkey: %s', tkey)
            # lg.debug(self.df_d[tkey].keys())
            logger.info('buffer_names_d[tkey].items: %s', self.buffer_names_d[tkey].items())
            self.writer_d[tkey].set_outfiles(list(self.buffer_names_d[tkey].keys()))
            for bn_key, bn_item in self.buffer_names_d[tkey].items():
                # lg.debug('bn_key: %s', bn_key)
                if self.cfg_kp_logic_ctrl['drop_for_output'] and self.progress_table_output_drop_fields:
                    self.df_d[tkey][bn_key].drop(columns=self.progress_table_output_drop_fields)
                lg.debug('len buffer %s: %s', bn_key, len(self.df_d[tkey][bn_key]))
                if (len(self.df_d[tkey][bn_key]) == 0):
                    lg.error('len(self.df_d[%s][%s]) == 0', tkey, bn_key)
                self.writer_d[tkey].set_buffer(bn_key, self.df_d[tkey][bn_key])
            self.writer_d[tkey].write()

    def generic_write_all(self):
        # lg.debug(pprint.pformat(self.tkeys_d))
        # XXX workaround, some dicts dt_d are None in other xlsx_framedumps groups
        self.generic_write_xlsx_group(0)
        self.generic_write_xlsx_group(1)
        # self.generic_write_xlsx_group(2) # debug sheet does not work
        # XXX later... do we need these groups at all?
        # xlsx_framedump_groups (1) was for looping old group names i think
        # and (0) for new group names
        for tk_i, tk_item in self.tkeys_d.items():
            pass
            # self.generic_write_xlsx_group(tk_i)

            """
            for tkey in tk_item:
                lg.info('=== write all for tkey: %s', tkey)
                lg.debug(self.df_d[tkey].keys())
                # logger.info('buffer_names_d[tkey].items: %s', self.buffer_names_d[tkey].items())
                self.writer_d[tkey].set_outfiles(self.buffer_names_d[tkey].keys())
                for bn_key, bn_item in self.buffer_names_d[tkey].items():
                    # lg.debug('bn_key: %s', bn_key)
                    if self.cfg_kp_logic_ctrl['drop_for_output'] and self.progress_table_output_drop_fields:
                        self.df_d[tkey][bn_key].drop(columns=self.progress_table_output_drop_fields)
                    # lg.debug('len buffer %s: %s', bn_key, len(self.df_d[tkey][bn_key]))
                    if (len(self.df_d[tkey][bn_key]) == 0):
                        lg.error('len(self.df_d[%s][%s]) == 0', tkey, bn_key)
                    self.writer_d[tkey].set_buffer(bn_key, self.df_d[tkey][bn_key])
                self.writer_d[tkey].write()
            """
