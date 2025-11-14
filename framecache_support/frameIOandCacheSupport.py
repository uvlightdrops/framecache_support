import os.path

from .dataBroker import DataBroker
try:
    import win32com.client
except:
    pass
from time import sleep
from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')
lg = setup_logger(__name__+'_2', __name__+'_2.log')
import pprint
# import sys

class FrameIOandCacheSupport(DataBroker):

    """ Class to support many dataframes IO and caching

    """

    def __init__(self):
        self.df_d = {}
        self.reader_d = {}
        self.writer_d = {}
        self.reader_single_d = {}
        self.writer_single_d = {}
        self.buffer_names_d = {}
        self.tkeys_d = {}
        self.frame_fields = {}

    # Neue Methode: configure erlaubt das bequeme Setzen mehrerer cfg_* Dicts
    def configure(self, **cfgs):
        """Setze Konfig-Maps (z.B. cfg_kp_frames, cfg_profile, cfg_kp_process_fields).

        Usage: fc.configure(cfg_kp_frames=..., cfg_profile=..., cfg_kp_si=...)
        """
        # XXX make my own
        for k, v in cfgs.items():
            # akzeptiere sowohl 'kp_frames' als auch 'cfg_kp_frames'
            if not k.startswith('cfg_'):
                setattr(self, 'cfg_' + k, v)
            else:
                setattr(self, k, v)
        # backward compatibility: ensure cfg_si is set if only cfg_kp_si present
        if not hasattr(self, 'cfg_si') and hasattr(self, 'cfg_kp_si'):
            self.cfg_si = getattr(self, 'cfg_kp_si')

    def get_frame_group(self, tkey):
        logger.debug('tkey: %s', tkey)
        if tkey not in self.df_d.keys():
            logger.error('50 No frame_group for tkey: %s', tkey)
            return None
        return self.df_d[tkey]

    def store_frame_group(self, tkey, df_g):
        self.df_d[tkey] = df_g
        # XXX if this is used again we need to handle this, a list comprehension?
        for key in df_g.keys():
            self.buffer_names_d[tkey][key] = key
        #self.buffer_names_d[tkey] = tkey
        logger.debug('60 Stored frame_group %s', tkey)

    def get_frame(self, tkey, group):
        logger.debug('get_frame tkey: %s, group: %s', tkey, group)
        logger.debug(self.df_d[tkey].keys())
        if group not in self.df_d[tkey].keys():
            logger.error('No frame for tkey: %s, group: %s', tkey, group)
            return None
        return self.df_d[tkey][group]

    def store_frame(self, tkey, group, df):
        logger.debug('store_frame tkey: %s, group: %s', tkey, group)
        #logger.debug(df.head(3))
        self.df_d[tkey][group] = df
        self.buffer_names_d[tkey][group] = group


    def init_framecache(self):
        xlsx_groups = [
            'xlsx_framedumps',
            'xlsx_framedumps_groups',
        ]
        tkeys_d = {}
        self.tkeys_all = []
        c = 0

        for group in self.cfg_kp_frames['supergroups']:
            tkeys = self.cfg_kp_frames[group]
            #tkeys = self.cfg_kp_process_fields[group]
            logger.debug('initializing for frameIO group: %s tkeys: %s  ', group, tkeys)

            self.init_dfio_dicts(tkeys)

            if not self.cfg_profile['reader'] is None:
                self.init_r(tkeys)
            if not self.cfg_profile['writer'] is None:
                self.init_w(tkeys)
            tkeys_d[c] = tkeys
            self.tkeys_all += tkeys
            c += 1
        self.tkeys_d = tkeys_d

    def init_fc(self):
        self.init_framecache()

    def init_fc_bytype(self):
        groups = [
            'reader_csv',
            'reader_db',
            'reader_excel',
        ]
        for group in groups:
            rws, type = group.split('_')
            tkeys = self.cfg_kp_frames[group]
            logger.debug('initializing for frameIO group: %s tkeys: %s  ', group, tkeys)
            for key in tkeys:
                if rws == 'reader':
                    self.reader_single_d[key] = self.init_reader_class_by_type(type)
                elif rws == 'writer':
                    self.writer_single_d[key] = self.init_writer_class_by_type(type)

    def init_dfio_dicts(self, tkeys):
        """ Initialize dicts of dicts of dataframes, readers, writers, buffer names"""
        for tkey in tkeys:
            #logger.debug('init_dfio_dicts: tkey: %s', tkey)
            self.df_d[tkey] = {}
            self.reader_d[tkey] = {}
            self.writer_d[tkey] = {}
            self.buffer_names_d[tkey] = {}

    # XXX own class for table and df support? Or DataBroker better
    def build_fieldlists(self, cfg):
        for key, value in cfg.items():
            if key.endswith('_table'):
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
        #logger.debug('self.fn* : %s', [attr for attr in dir(self) if attr.startswith('fn_')])
        # logger.debug(self.frame_fields['entries_raw_table'])

    def init_r(self, tkeys):
        #logger.debug('init_r for tkeys: %s', tkeys)
        self.reader = self.init_reader_class()
        self.reader.set_src_dir(self.cfg_si['data_in_sub'])
        for tkey in tkeys:
            self.reader_d[tkey] = None

    def init_w(self, tkeys):
        # legacy writer single object
        self.writer = self.init_writer_class()
        for tkey in tkeys:
            self.writer_d[tkey] = None

    def get_reader_group(self, tkey, *args, **kwargs):
        if self.reader_d.get(tkey) is None:
            reader = self.init_reader_class(*args, **kwargs)
            reader.set_name(tkey)
            self.reader_d[tkey] = reader
        return self.reader_d[tkey]

    def get_writer_group(self, tkey, *args, **kwargs):
        if self.writer_d.get(tkey) is None:
            writer = self.init_writer_class(*args, **kwargs)
            #logger.debug('writer init success?: %s', writer)
            writer.set_name(tkey)
            # DEV test here
            writer.set_dst(self.cfg_si['data_out_sub'].joinpath(self.phase_subdir, '_'+tkey))
            writer.init_writer_all()
            self.writer_d[tkey] = writer
        return self.writer_d[tkey]

    def get_reader(self, tkey, *args, **kwargs):
        if self.reader_single_d[tkey] is None:
            logger.debug('init single reader for tkey: %s was None in cache', tkey)
            reader = self.init_reader_class(*args, **kwargs)
            reader.set_name(tkey)
            self.reader_single_d[tkey] = reader
        return self.reader_single_d[tkey]

    def get_writer(self, tkey, *args, **kwargs):
        if self.writer_single_d.get(tkey, None) is None:
            logger.debug('init single writer or tkey: %s was None in cache', tkey)
            writer = self.init_writer_class(*args, **kwargs)
            writer.set_name(tkey)
            self.writer_single_d[tkey] = writer
        return self.writer_single_d[tkey]

    def prep_writer(self):
        logger.debug('self.phase_subdir: %s', self.phase_subdir)
        p = self.cfg_si['data_out_sub'].joinpath(self.phase_subdir)
        if not p.exists():
            os.makedirs(self.cfg_si['data_out_sub'].joinpath(self.phase_subdir))

        for tk_i, tk_item in self.tkeys_d.items():
            for tkey in tk_item:
                # XXX os.remove here?
                logger.debug('prep writer for %s', tkey)
                outfiles = list(self.buffer_names_d[tkey].keys())
                self.writer_d[tkey].set_outfiles(outfiles)
                self.writer_d[tkey].set_dst(self.cfg_si['data_out_sub'].joinpath(self.phase_subdir, '_'+tkey))
                # lg.debug('buffer_names_d[%s].keys(): %s', tkey, self.buffer_names_d[tkey].keys())
                for name in self.buffer_names_d[tkey].keys():
                    lg.debug('set buffer len=%s for %s', len(self.df_d[tkey[name]]), name)
                #    self.writer_d[tkey].set_buffer(name, self.df_d[tkey][name])
                setattr(self.writer_d[tkey], 'cfg_si', self.cfg_si)
                self.writer_d[tkey].init_writer_all()

    # NEU
    #def write_frame_group(self, tkey: str, df_d) -> None:
    def write_frame_group(self, tkey: str): #, df_d) -> None:
        logger.debug('self.buffer_names_d[tkey]: %s - tkey is %s', self.buffer_names_d[tkey], tkey)
        self.get_writer_group(tkey)
        outnames = list(self.buffer_names_d[tkey].keys())
        logger.debug('outnames: %s', outnames)
        self.writer_d[tkey].set_outfiles(outnames)

        for bn_key, bn_item in self.buffer_names_d[tkey].items():
            logger.debug('len buffer %s: %s', bn_key, len(self.df_d[tkey][bn_key]))
            if (len(self.df_d[tkey][bn_key]) == 0):
                lg.error('len(self.df_d[%s][%s]) == 0', tkey, bn_key)
            self.writer_d[tkey].set_buffer(bn_key, self.df_d[tkey][bn_key])
            #self.writer_d[tkey].set_buffer(bn_key, df_d[bn_key])
        self.writer_d[tkey].write()

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
            """
