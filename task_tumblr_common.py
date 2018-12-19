# -*- coding: utf-8 -*-
# encoding=utf-8
import os
import re
import glob
import pickle
import operator
import threading
from task_spider_common import CFile


class TumblrCommon:
    ROOT_DIR = 'G:\\'

    tumblrdir = os.path.join(ROOT_DIR, 'tumblr')
    xunleidir = os.path.join(tumblrdir, 'xunlei')

    taskerdir = os.path.join(ROOT_DIR, 'tumblrtasker')
    cacherdir = os.path.join(taskerdir, '.tumblrcacher')
    mmediadir = os.path.join(cacherdir, '.mmedia')
    keyurldir = os.path.join(cacherdir, '.keyurl')

    tdatabdir = os.path.join('G:\\', 'tumblrtasker', '.tumblrcacher')
    tdataname = 'tumblr.db'
    tmmediadb = 'tumblr_mmedias.db'
    tmaxqsize = 10000

    gblogname = list()

    @classmethod
    def init(cls):
        cls._init_tumblr_dirs()
        cls.blognamelspath = os.path.join(cls.taskerdir, 'tumblr_blog_namels.txt')
        cls.gblogname = cls._get_tumblr_blognames(cls.blognamelspath)

    @classmethod
    def _init_tumblr_dirs(cls):
        CFile.mkdirs(cls.tumblrdir)
        CFile.mkdirs(cls.mmediadir)
        CFile.mkdirs(cls.keyurldir)
        CFile.mkdirs(cls.tdatabdir)

    @classmethod
    def _get_tumblr_blognames(cls, _blognames_path):
        _blogstring = CFile.read_string(_blognames_path)
        _blogstring = _blogstring.lower().strip(' ').strip('\n')
        _blogstring = re.sub('[\r\t]', '', _blogstring)
        _blognames = list(sorted(set(_blogstring.split('\n'))))
        if operator.ne(_blogstring, '\n'.join(_blognames)):
            CFile.write_string(cls.blognamelspath, '\n'.join(_blognames))
        return _blognames

    @classmethod
    def update_tumblr_blognames(cls, _new_blognames=list()):
        _blognames = cls._get_tumblr_blognames(cls.blognamelspath)
        if not list(set(_new_blognames).difference(set(_blognames))):
            return _blognames
        _blognames.extend(_new_blognames)
        CFile.write_string(cls.blognamelspath, '\n'.join(_blognames))
        return _blognames

    @classmethod
    def uninit(cls):
        cls.update_tumblr_blognames(cls.gblogname)

    @classmethod
    def get_mmediadb_path(cls):
        return os.path.join(cls.tdatabdir, cls.tmmediadb)

    @classmethod
    def get_keyurl_path(cls, _blogname='total'):
        if _blogname == 'total':
            _fname = _blogname + '_keyurl' + '_completed' + '.txt'
            return os.path.join(TumblrCommon.cacherdir, _fname)
        else:
            _fname = _blogname + '_keyurl' + '.txt'
            return os.path.join(cls.keyurldir, _fname)

    @classmethod
    def get_keyurl_paths(cls, _blognames=list()):
        if _blognames:
            return list(map(cls.get_keyurl_path, _blognames))
        else:
            return glob.glob(os.path.join(cls.keyurldir, '*keyurl.txt'))

    @classmethod
    def get_ariauri_path(cls, _type='photo', _ext='uri'):
        _fname = 'mkeydir' + '_' + 'total' + '_' + str(_type) + '_' + str('all') + '.' + _ext
        return os.path.join(cls.cacherdir, _fname)

    @classmethod
    def get_mmedia_path(cls, _blogname='total'):
        if _blogname == 'total':
            _fname = _blogname + '_mmedia' + '_completed' + '.txt'
            return os.path.join(TumblrCommon.cacherdir, _fname)
        else:
            _fname = _blogname + '_mmedia' + '.txt'
            return os.path.join(cls.mmediadir, _fname)

    @classmethod
    def get_mmedia_paths(cls, _blognames=list()):
        if _blognames:
            return list(map(cls.get_mmedia_path, _blognames))
        else:
            return glob.glob(os.path.join(cls.mmediadir, '*_mmedia.txt'))

    @classmethod
    def get_total_mkeydir_path(cls):
        _fname = 'total' + '_mkeydir' + '.csv'
        return os.path.join(TumblrCommon.tdatabdir, _fname)

    @classmethod
    def get_task_ctrl_path(cls):
        return os.path.join(cls.cacherdir, 'tumblr_task_ctrl' + '.txt')


class TumblrControl(object):
    _glock = threading.RLock()

    def __new__(cls, *args, **kwargs):
        cls._glock.acquire()
        if not hasattr(cls, '_instance'):
            cls._instance = super(TumblrControl, cls).__new__(cls)
        cls._glock.release()
        return cls._instance

    def __init__(self):
        self._lock = threading.Lock()
        self.taskerdir = TumblrCommon.taskerdir
        self._ctrlpath = TumblrCommon.get_task_ctrl_path()
        self._saverate = 50
        self._upscount = 0
        self._ctrldata = dict()
        self._ctrlfunc = dict({
            'addon': self.__addon,
            'addons': self.__addons,
            'query': self.__query,
            'update': self.__update
        })
        self.enable = False

    def argparse(self, _argv):
        if '-u' in _argv and not self.enable:
            self.enable = True
        self.enable = False
        self._ctrldata = self.__read()
        if self._ctrldata and self._ctrldata.get('additional'):
            _additional = self._ctrldata['additional']
            if not self._ctrldata['additional']:
                print(_additional)

        _bcompleted = not self._ctrldata or self._ctrldata.setdefault('completed', False) or not \
            self._ctrldata.get('argv') or not self.enable
        _additional = list() if not self._ctrldata or not self.enable else \
            self._ctrldata.setdefault('additional', list()).copy()
        if _bcompleted:
            self._ctrldata = dict()
            self._ctrldata['argv'] = _argv
            self._ctrldata['additional'] = _additional
            self.save()

        return self._ctrldata['argv']

    def save(self):
        if self.enable:
            CFile.write_json(self._ctrlpath, self._ctrldata)

    def operator(self, _operater, _value, *tpaths):
        if not self._ctrlfunc.get(_operater):
            raise TypeError
        _func = self._ctrlfunc[_operater]
        return _func(_value, *tpaths)

    def throperator(self, _operater, _value, *tpaths):
        with self._lock:
            return self.operator(_operater, _value, *tpaths)

    def __read(self):
        try:
            _ctrldata = CFile.read_pickle(self._ctrlpath)
        except pickle.PickleError:
            _ctrldata = CFile.read_json(self._ctrlpath)
        return _ctrldata

    def __write(self):
        if self.enable:
            CFile.write_pickle(self._ctrlpath, self._ctrldata)

    def __updatecount(self):
        self._upscount = self._upscount + 1
        if self._upscount % self._saverate == 0:
            self._upscount = 0
            self.__write()

    def setrunning(self, _running=True):
        return self.tumblr_control('running', 'set', _running)

    def isrunning(self):
        return self.tumblr_control('running', 'get', True)

    def tumblr_control(self, _conf_flag, _conf_operate, _conf_value):
        _conf_path = os.path.join(self.taskerdir, 'tumblr_' + _conf_flag + '.conf')
        _value = os.path.exists(_conf_path) and os.path.isfile(_conf_path)
        if _conf_operate == 'get':
            return _value
        elif _conf_operate == 'set':
            if _conf_value:
                if not _value:
                    open(_conf_path, "w+").close()
            else:
                if _value:
                    os.remove(_conf_path)
            return _conf_value
        raise False

    def __query(self, _value, *tpaths):
        _tpath = '.'.join(tpaths)
        return self._ctrldata.setdefault(_tpath, _value)

    def __update(self, _value, *tpaths):
        _tpath = '.'.join(tpaths)
        self._ctrldata[_tpath] = _value
        self.__updatecount()
        return _value

    def __addon(self, _value, *tpaths):
        self._ctrldata['additional'].append(_value)
        self.__updatecount()
        return _value

    def __addons(self, _values, *tpaths):
        self._ctrldata['additional'].expend(_values)
        self.__updatecount()
        return _values
