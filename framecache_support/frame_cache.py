from __future__ import annotations

import threading
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from flowpy.utils import setup_logger

logger = setup_logger(__name__, __name__ + '.log')


class FrameCache:
    """Lightweight, lazy-initializing cache for pipeline dataframes and IO helpers.

    Ziele / Designentscheidungen:
    - Keine zentrale `init_framecache()` mehr: Ressourcen (dicts, reader/writer)
      werden on-demand (lazy) angelegt.
    - Dependency injection: Reader/Writer-Factories von außen setzen.
    - Thread-sicher (RLock) für parallele Zugriffe.
    - Methoden sind bewusst einfach gehalten, für spätere Spezialisierung.

    Kern-API (Kurz):
    - configure(**cfgs)
    - get_frame_group(tkey) / store_frame_group(tkey, df_d)
    - get_frame(tkey, group) / store_frame(tkey, group, df)
    - get_writer(tkey) / set_writer_factory(callable)
    - write_frame_group(tkey) / write_all()
    - clear(tkey=None)
    """

    def __init__(self) -> None:
        # lazy containers
        self._df_d: Dict[str, Dict[str, Any]] = {}
        self._reader_d: Dict[str, Any] = {}
        self._writer_d: Dict[str, Any] = {}
        self._buffer_names_d: Dict[str, Dict[str, str]] = {}

        # config placeholders (set via configure)
        self.cfg: Dict[str, Any] = {}

        # factories must return a new reader/writer instance when called with no args
        self._reader_factory: Optional[Callable[[], Any]] = None
        self._writer_factory: Optional[Callable[[], Any]] = None

        # lock for thread-safety
        self._lock = threading.RLock()

    # -------------------- configuration / factories --------------------
    def configure(self, **cfgs: Any) -> None:
        """Setze Konfigurationen. Erwartet dicts wie cfg_kp_frames, cfg_si, cfg_profile.

        Beispiel: cache.configure(cfg_kp_frames=..., cfg_si=..., cfg_profile=...)
        """
        with self._lock:
            self.cfg.update(cfgs)

    def set_reader_factory(self, factory: Callable[[], Any]) -> None:
        """Setze Factory, die ein Reader-Objekt erzeugt."""
        self._reader_factory = factory

    def set_writer_factory(self, factory: Callable[[], Any]) -> None:
        """Setze Factory, die ein Writer-Objekt erzeugt."""
        self._writer_factory = factory

    # -------------------- internal helpers --------------------
    def _ensure_tkey(self, tkey: str) -> None:
        """Stelle sicher, dass interne dicts für tkey existieren (lazy init)."""
        if tkey not in self._df_d:
            self._df_d[tkey] = {}
            self._reader_d[tkey] = None
            self._writer_d[tkey] = None
            self._buffer_names_d[tkey] = {}

    # -------------------- frame group API --------------------
    def get_frame_group(self, tkey: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._df_d.get(tkey)

    def store_frame_group(self, tkey: str, df_d: Dict[str, Any]) -> None:
        with self._lock:
            self._ensure_tkey(tkey)
            self._df_d[tkey].update(df_d)
            # register buffer names lazily
            for name in df_d.keys():
                self._buffer_names_d[tkey].setdefault(name, name)
            logger.debug('Stored frame_group %s keys=%s', tkey, list(df_d.keys()))

    # -------------------- single frame API --------------------
    def get_frame(self, tkey: str, group: str) -> Optional[Any]:
        with self._lock:
            if tkey not in self._df_d:
                return None
            return self._df_d[tkey].get(group)

    def store_frame(self, tkey: str, group: str, df: Any) -> None:
        with self._lock:
            self._ensure_tkey(tkey)
            self._df_d[tkey][group] = df
            self._buffer_names_d[tkey].setdefault(group, group)
            logger.debug('store_frame tkey=%s group=%s len=%s', tkey, group, getattr(df, 'shape', 'n/a'))

    # -------------------- reader/writer helpers --------------------
    def _create_reader(self) -> Any:
        if not self._reader_factory:
            raise RuntimeError('Reader factory not configured')
        return self._reader_factory()

    def _create_writer(self) -> Any:
        if not self._writer_factory:
            raise RuntimeError('Writer factory not configured')
        return self._writer_factory()

    def get_reader(self, tkey: str) -> Any:
        with self._lock:
            self._ensure_tkey(tkey)
            if self._reader_d[tkey] is None:
                r = self._create_reader()
                # optional: name and cfg wiring if supported by reader
                try:
                    r.set_name(tkey)
                except Exception:
                    pass
                self._reader_d[tkey] = r
            return self._reader_d[tkey]

    def get_writer(self, tkey: str) -> Any:
        with self._lock:
            self._ensure_tkey(tkey)
            if self._writer_d[tkey] is None:
                w = self._create_writer()
                try:
                    w.set_name(tkey)
                except Exception:
                    pass
                # optional: set destination from cfg if available
                dst = None
                try:
                    dst = self.cfg.get('cfg_si', {}).get('data_out_sub')
                except Exception:
                    dst = None
                if dst is not None:
                    try:
                        # writer may expect a Path
                        w.set_dst(Path(dst))
                    except Exception:
                        pass
                self._writer_d[tkey] = w
            return self._writer_d[tkey]

    # -------------------- write operations --------------------
    def write_frame_group(self, tkey: str) -> None:
        """Schreibe alle buffered frames für tkey via zugeordnetem Writer.

        Writer-Interface: set_outfiles(list), set_buffer(name, df), write()
        """
        with self._lock:
            if tkey not in self._df_d:
                logger.error('write_frame_group: unknown tkey %s', tkey)
                return
            writer = self.get_writer(tkey)
            outfiles = list(self._buffer_names_d[tkey].keys())
            try:
                writer.set_outfiles(outfiles)
            except Exception:
                pass
            for bn_key in outfiles:
                df = self._df_d[tkey].get(bn_key)
                if df is None:
                    logger.warning('No buffer %s for tkey %s', bn_key, tkey)
                    continue
                try:
                    writer.set_buffer(bn_key, df)
                except Exception:
                    logger.exception('Writer.set_buffer failed for %s/%s', tkey, bn_key)
            try:
                writer.write()
            except Exception:
                logger.exception('Writer.write failed for tkey %s', tkey)

    def write_all(self) -> None:
        with self._lock:
            for tkey in list(self._df_d.keys()):
                self.write_frame_group(tkey)

    # -------------------- housekeeping --------------------
    def clear(self, tkey: Optional[str] = None) -> None:
        """Lösche Cache für tkey oder komplett wenn tkey None."""
        with self._lock:
            if tkey is None:
                self._df_d.clear()
                self._reader_d.clear()
                self._writer_d.clear()
                self._buffer_names_d.clear()
            else:
                self._df_d.pop(tkey, None)
                self._reader_d.pop(tkey, None)
                self._writer_d.pop(tkey, None)
                self._buffer_names_d.pop(tkey, None)


# small example usage in docstring style (not executed):
# cache = FrameCache()
# cache.configure(cfg_si={'data_out_sub': '/tmp/out'})
# cache.set_writer_factory(lambda: MyWriter())
# cache.store_frame('entries', 'raw', df)
# cache.write_frame_group('entries')

