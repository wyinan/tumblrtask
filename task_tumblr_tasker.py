# -*- coding: utf-8 -*-
# encoding=utf-8
import os
import re
import gc
import glob
import time
import shutil
import numpy as np
import pandas as pd
import sqlite3
import threadpool
from task_common import RALog
from datetime import datetime, timedelta
from task_spider_common import CUtil, CFile
from task_tumblr_common import TumblrCommon, TumblrControl
from task_tumblr_sqlite import TumblrSqlite
from task_tumblr_update import TumblrUpdater
from task_tumblr_parser import TumblrParser


class Tasker:

    @staticmethod
    def get_update_blognames_fromdb(_all_blognames):
        _bloginfo = TumblrSqlite.select_blogs()
        _lost_blognames = list(map(lambda _blog: _blog['blogname'], _bloginfo))
        _need_blognames = list(set(_all_blognames).difference(set(_lost_blognames)))
        return _need_blognames

    @staticmethod
    def get_all_blognames_fromdb():
        _bloginfo = TumblrSqlite.select_all_blogs()
        _blognames = list(map(lambda _blog: _blog['blogname'], _bloginfo))
        return _blognames

    @classmethod
    def move_media_to_tumblr(cls, _lsmdir, _mldir, _mkdict=dict()):
        for _dir in _lsmdir:
            for _root, _dirs, _files in os.walk(_dir, topdown=True):
                RALog.i('[MOVE] %s' % _root)
                for _file in _files:
                    if not _file.startswith('tumblr_'):
                        _key, _type = cls.get_key_and_type(_file)
                    else:
                        _key, _type = cls.get_key_and_type(_file)
                    if not _key or not _type:
                        continue
                    elif os.path.exists(os.path.join(_root, _file + '.aria2')):
                        os.remove(os.path.join(_root, _file + '.aria2'))
                        os.remove(os.path.join(_root, _file))
                        continue
                    if _key in _mkdict:
                        _mkeydir = _mkdict[_key]
                        _ftargetdir = os.path.join(_mldir, _mkeydir)
                    elif _root.endswith('xunlei'):
                        continue
                    else:
                        _ftargetdir = os.path.join(_mldir, _type, 'xunlei')
                    if _root == _ftargetdir:
                        continue
                    cls.move_media_file(os.path.join(_root, _file), _ftargetdir, _file)
        return

    @staticmethod
    def move_media_file(_fsource, _targetdir, _targetname):
        _targetfile = os.path.join(_targetdir, _targetname)
        RALog.i('%s, %s' % (_fsource, _targetfile))
        try:
            shutil.move(_fsource, _targetfile)
        except FileNotFoundError:
            CFile.mkdirs(_targetdir)
            shutil.move(_fsource, _targetfile)
        except FileExistsError as err:
            RALog.e('%s, %s' % (_targetfile, str(err)))
        except Exception as err:
            RALog.e('%s, %s' % (_targetfile, str(err)))
        return

    @staticmethod
    def get_key_and_type(_file_name):
        _redict = dict({
            r'^tumblr_(inline_|static_)?[0-9a-zA-Z]{18}([0-9]{1,2})?(_r[0-9]{1,2})?(_[0-9]{3,4})?.(gif|jpg|png)?$':
                (lambda _ukey: _ukey[7:7 + 18], 'photo'),
            r'^[0-9a-zA-Z]{1,90}(_r[0-9]{1,2})?(_[0-9]{3,4})?.(gif|jpg|png)?$':
                (lambda _ukey: _ukey.split('_')[0], 'photo'),
            r'^tumblr_[0-9a-zA-Z]{17}(_r[0-9]{1,2})?(_[0-9]{3,4})?.(mp4|mov)?$':
                (lambda _ukey: _ukey[7:7 + 17], 'video')
        })
        for _reurl, _keytype in _redict.items():
            _compile = re.compile(_reurl)
            _match = _compile.match(_file_name)
            if not _match:
                continue
            _key = _keytype[0](_file_name)
            _type = _keytype[1]
            return _key, _type
        return '', ''

    @classmethod
    def _run_task_perpare(cls, _taskname, args, _blogitems, _thrcnt=0):
        if not _blogitems:
            return
        _blogcnt = len(_blogitems)
        _tumblrs = list([TumblrUpdater(_taskname, args, _blogitem, CUtil.placeholder(_index, _blogcnt))
                         for _index, _blogitem in enumerate(_blogitems)])
        if not _thrcnt:
            _thrcnt = min(args.processes, max(1, int(_blogcnt / 4)))
        cls._run_task_thread_pool(_tumblrs, _thrcnt)
        return

    @classmethod
    def _run_tasker(cls, _tasker):
        return _tasker.run_tumblr()

    @classmethod
    def _run_task_thread_pool(cls, _taskers, _thrcnt):
        _pool = threadpool.ThreadPool(_thrcnt)
        _requests = threadpool.makeRequests(cls._run_tasker, _taskers)
        [_pool.putRequest(_req) for _req in _requests]
        _pool.wait()
        return


class TumblrTasker(Tasker):
    tucommon = TumblrCommon()
    tusqlite = TumblrSqlite()
    taskctrl = TumblrControl()

    @classmethod
    def init(cls, _argv):
        cls.tucommon.init()
        cls.tusqlite.init_tumblr(
            os.path.join(cls.tucommon.tdatabdir, cls.tucommon.tdataname),
            cls.tucommon.tmaxqsize)
        return cls.taskctrl.argparse(_argv)

    @classmethod
    def uninit(cls):
        cls.taskctrl.save()
        cls.tusqlite.uninit()
        cls.tucommon.uninit()
        return

    @classmethod
    def run_task(cls, args, _sites):
        _taskdict = dict({
            'discover': (args.discover, cls._task_discover),
            'update': (args.update, cls._task_update),
            'rebelong': (args.rebelong, cls._task_rebelong),
            'remove': (args.remove, cls._task_remove),
            'uri': (args.uri, cls._task_uri)
        })

        if not args.start:
            _dindis = 90
            _now = datetime.now()
            args.start = (_now - timedelta(days=_dindis)).strftime('%Y-%m-%d')

        cls.taskctrl.setrunning()
        _blogitems = TumblrParser.findtumblrblogitems(_sites, _postid=True)
        for _taskname, _tasktuple in _taskdict.items():
            if not _tasktuple[0]:
                continue
            if not cls.taskctrl.operator('query', False, _taskname, 'completed'):
                _tasktuple[1](_taskname, args, _blogitems)
                cls.taskctrl.operator('update', True, _taskname, 'completed')
                if not cls.taskctrl.isrunning():
                    break

        if cls.taskctrl.isrunning():
            cls.taskctrl.operator('update', True, 'completed')

        cls.taskctrl.save()

    @classmethod
    def __collect_tumblr_mmedias(cls, _taskname, args):
        _fmmedias = cls.tucommon.get_mmedia_paths()
        _mmedias = dict()
        _count = len(_fmmedias)
        for _index, _fmmedia in enumerate(_fmmedias):
            RALog.i('[%s] [%s] [%s] %s' %
                    (_taskname, 'mmedia', CUtil.placeholder(_index, _count), _fmmedia))
            _mmedias.update(CFile.read_json(_fmmedia))
        return _mmedias

    @classmethod
    def __update_tumblr_mmedias(cls, _taskname, args):
        _allmmedias = cls.__collect_tumblr_mmedias(_taskname, args)
        if not _allmmedias:
            return dict()

        _allkeys = list(_allmmedias.keys())

        _fmkeydir = TumblrCommon.get_total_mkeydir_path()
        _dfkey = pd.read_csv(_fmkeydir, sep=',', header=0, index_col=None)
        _mkeys = np.array(_dfkey['key']).tolist()

        RALog.i('[%s] [%s] [%s] %s' %
                (_taskname, 'mmedia', 'mmedias', 'all mmedias len: %d' % len(_allmmedias)))

        _newmmedias = dict()
        _newmkeydir = list()
        _mnkey = list(set(_allkeys) - set(_mkeys))
        
        for _mkey in _mnkey:
            _mmedia = _allmmedias[_mkey]
            _newmmedias[_mkey] = _mmedia
            _newmkeydir.append({'key': _mkey, 'out': _mmedia[2] + '\\' + _mmedia[1]})

        RALog.i('[%s] [%s] [%s] %s' %
                (_taskname, 'mmedia', 'mmedias', 'new mmedias len: %d' % len(_newmmedias)))

        _mkeys = list()
        _allmmedias = dict()
        gc.collect()

        if _newmkeydir:
            _dfkey = _dfkey.append(_newmkeydir, ignore_index=True)
            RALog.i('[%s] [%s] %s' % (_taskname, 'dfkey', 'sort_values'))
            _dfkey = _dfkey.sort_values(by=['key', 'out'], ascending=True)
            RALog.i('[%s] [%s] %s' % (_taskname, 'dfkey', 'drop_duplicates'))
            _dfkey = _dfkey.drop_duplicates()
            RALog.i('[%s] [%s] %s' % (_taskname, 'dfkey', 'to_csv'))
            _dfkey.to_csv(_fmkeydir, index=False, encoding='gbk')

        CFile.write_json(_fmkeydir + '.bak', _newmkeydir, _sort_keys=True)

        _newmkeydir = dict()
        gc.collect()

        _fmmedia = cls.tucommon.get_mmedia_path()
        CFile.write_json(_fmmedia, _newmmedias, _sort_keys=True)

        return _newmmedias

    @classmethod
    def __update_tumblr_mediadb(cls, _taskname, args, _mmedias):
        _fdbm = cls.tucommon.get_mmediadb_path()
        _conn = sqlite3.connect(_fdbm)

        _fields = [
            'id     INTEGER PRIMARY KEY AUTOINCREMENT',
            'key    NOT NULL',
            'name   NOT NULL',
            'type   NOT NULL',
            'likes',
            'updated',
            'outs',
            'urls',
            'exist False'
        ]

        _conn.execute('PRAGMA synchronous = OFF')
        _conn.execute('CREATE TABLE IF NOT EXISTS %s ( %s )' % ('mmedia', ',\n'.join(_fields)))
        _conn.commit()

        _sql = 'REPLACE INTO %s (%s) VALUES (?, ?, ?, ?, ?, ?, ?, ?)' % \
               ('mmedia', 'key, name, type, likes, updated, outs, urls, exist')

        _sql = 'INSERT OR IGNORE INTO %s (%s) VALUES (?, ?, ?, ?, ?, ?, ?, ?)' % \
               ('mmedia', 'key, name, type, likes, updated, outs, urls, exist')

        _param = list()
        _count = len(_mmedias)
        for _index, _mmedia in enumerate(_mmedias.values()):
            _param.append(_mmedia)
            if (_index + 1) % 5000 == 0:
                RALog.i('[%s] [%s] [%s] %s' %
                        (_taskname, 'mmedia', CUtil.placeholder(_index, _count), 'tumblr_mmedia'))
                _conn.executemany(_sql, _param)
                _conn.commit()
                _param = []

        if _param:
            _conn.executemany(_sql, _param)
            _conn.commit()
        _conn.close()

        return

    @classmethod
    def __update_tumblr_ariauri(cls, _taskname, args, _mmedias):

        _ariavuri = list()
        _ariapuri = list()
        _vuri = list()
        _puri = list()

        _count = len(_mmedias)
        for _index, _mmedia in enumerate(_mmedias.values()):
            if (_index + 1) % 5000 == 0:
                RALog.i('[%s] [%s] [%s] %s' %
                        (_taskname, 'mmedia', CUtil.placeholder(_index, _count), 'tumblr_ariauri'))
            _outs = list(map(lambda _item: _item.strip('\r\n'), _mmedia[5].split(',')))
            _urls = list(map(lambda _item: _item.strip('\r\n'), _mmedia[6].split(',')))
            _uris = list()
            for _num, _out in enumerate(_outs):
                if os.path.isfile('K:\\tumblr\\' + _out) \
                        and not os.path.isfile(os.path.join('K:\\tumblr\\', _out, '.aria2')):
                    continue
                else:
                    _uris.append(_urls[_num] + '\n out=' + _out)

            if not _uris:
                continue
            if _mmedia[2] == 'video':
                _ariavuri.extend(_uris)
                _vuri.extend(_urls)
            else:
                _ariapuri.extend(_uris)
                _puri.extend(_urls)

        _fariapuri = cls.tucommon.get_ariauri_path(_type='photo', _ext='uri')
        _fariavuri = cls.tucommon.get_ariauri_path(_type='video', _ext='uri')

        CFile.write_list(_fariapuri, _ariapuri)
        RALog.i('[%s] [%s] [%s] %s' % (_taskname, 'mmedia', 'uri', _fariapuri))

        CFile.write_list(_fariavuri, _ariavuri)
        RALog.i('[%s] [%s] [%s] %s' % (_taskname, 'mmedia', 'uri', _fariavuri))

        _fariapuri = cls.tucommon.get_ariauri_path(_type='photo', _ext='txt')
        _fariavuri = cls.tucommon.get_ariauri_path(_type='video', _ext='txt')

        CFile.write_list(_fariapuri, _puri)
        RALog.i('[%s] [%s] [%s] %s' % (_taskname, 'mmedia', 'url', _fariapuri))

        CFile.write_list(_fariavuri, _vuri)
        RALog.i('[%s] [%s] [%s] %s' % (_taskname, 'mmedia', 'url', _fariapuri))

        return

    @classmethod
    def _task_update(cls, _taskname, args, _blogitems):
        
        _blognames = cls.taskctrl.operator(
            'query',
            _blogitems if _blogitems else TumblrParser.blognamestoblogitems(TumblrCommon.gblogname),
            _taskname, 'blognames')

        if not _blognames:
            return

        # perpare update for new blogname
        cls._run_task_perpare(_taskname, args, _blognames)
        cls.taskctrl.operator('update', list(), _taskname, 'blognames')

        # mkey rebelong and get new mmedias
        _mmedias = cls.__update_tumblr_mmedias(_taskname, args)
        if not _mmedias:
            return

        # update tumblr mediadb
        cls.__update_tumblr_mediadb(_taskname, args, _mmedias)
        
        # output tumblr ariauri
        cls.__update_tumblr_ariauri(_taskname, args, _mmedias)

        # clean mmedias file
        _fmmedias = glob.glob(os.path.join(TumblrCommon.mmediadir, '*_mmedia.txt'))
        for _fmmedia in _fmmedias:
            os.remove(_fmmedia)

        return

    @classmethod
    def _task_rebelong(cls, _taskname, args, _blogitems):

        """
        sqlite3.exe -list tumblr_mmedias.db "select key, type, name from mmedia" > total_mkeydir_list.txt
        """

        _chdir = TumblrCommon.tdatabdir
        _flist = os.path.join(_chdir, 'total_mkeydir_list.txt')
        _fdict = os.path.join(_chdir, 'total_mkeydir.csv')

        _dfkey = pd.DataFrame(columns=['key', 'out'])

        RALog.i('[%s] mkeydir: %s' % (_taskname, _flist))
        _file = open(_flist)

        _index = 0
        while True:
            _lines = _file.readlines(10000 * 100)
            if not _lines:
                break
            _keyls = list()
            for _line in _lines:
                _mktn = _line.strip('\n').split('|')
                _mkey = _mktn[0].split(',')
                for _key in _mkey:
                    _keyls.append({'key': _key, 'out': _mktn[1] + '\\' + _mktn[2]})

            RALog.i('[%s] dfkey: %d, %d' % (_taskname, _index, len(_keyls)))
            _index = _index + 1
            _dfkey = _dfkey.append(_keyls, ignore_index=True)
            _keyls = list()

        _file.close()

        RALog.i('[%s] [%s] %s' % (_taskname, 'dfkey', 'sort_values'))
        _dfkey = _dfkey.sort_values(by=['key', 'out'], ascending=True)
        RALog.i('[%s] [%s] %s' % (_taskname, 'dfkey', 'drop_duplicates'))
        _dfkey = _dfkey.drop_duplicates()
        RALog.i('[%s] [%s] %s' % (_taskname, 'dfkey', 'to_csv'))
        _dfkey.to_csv(_fdict, index=False, encoding='gbk')

        return

    @classmethod
    def _task_remove(cls, _taskname, args, _blogitems):

        _malib = {
            'G': {'source': list(['K:\\tumblr']),
                  'target': 'K:\\tumblr'
                  },
            'H': {'source': list(['K:\\tumblr\\xunlei']),
                  'target': 'K:\\tumblr',
                  },
            'I': {'source': list(['K:\\tumblr\\photo\\xunlei']),
                  'target': 'K:\\tumblr',
                  },
            'J': {'source': list(['K:\\tumblr\\photos\\xunlei']),
                  'target': 'K:\\tumblr',
                  },
            'L': {'source': list(['K:\\tumblr\\gif\\xunlei']),
                  'target': 'K:\\tumblr',
                  },
            'M': {'source': list(['K:\\tumblr\\gifs\\xunlei']),
                  'target': 'K:\\tumblr',
                  },
            'N': {'source': list(['K:\\tumblr\\video\\xunlei']),
                  'target': 'K:\\tumblr',
                  }
        }

        del (_malib['G'])

        _fmkeydir = TumblrCommon.get_total_mkeydir_path()
        RALog.i('[%s] read_csv: %s' % (_taskname, _fmkeydir))
        _dfkey = pd.read_csv(_fmkeydir, sep=',', header=0, index_col=None)
        RALog.i('[%s] to_dict: %s' % (_taskname, 'mkeydir'))
        _hmkeydir = _dfkey.set_index('key').T.to_dict('records')[0]
        _dfkey = pd.DataFrame()

        for _driver, _libdir in _malib.items():
            cls.move_media_to_tumblr(_libdir['source'], _libdir['target'], _hmkeydir)

        return

    @classmethod
    def _task_uri(cls, _taskname, args, _blogitems):

        _chdir = TumblrCommon.tdatabdir
        _usrs = CFile.read_list(os.path.join(_chdir, 'diyusrls.txt'))
        _usrs = list(sorted(set(_usrs)))

        _tflag = time.strftime('%Y-%m-%d_%H-%M-%S')
        RALog.i('[%s] [%s] %s' % (_taskname, _tflag, str(_usrs)))

        _troot = TumblrCommon.tumblrdir
        _droot = TumblrCommon.xunleidir

        CFile.mkdirs(os.path.join(_chdir, '.txt'))
        CFile.mkdirs(os.path.join(_chdir, '.uri'))
        CFile.mkdirs(os.path.join(_chdir, '.url'))
        _ftxt = os.path.join(_chdir, '.txt', 'diy_txt[%s].txt' % _tflag)
        _furi = os.path.join(_chdir, '.uri', 'diy_uri[%s].txt' % _tflag)
        _furl = os.path.join(_chdir, '.url', 'diy_url[%s].txt' % _tflag)

        # sqlite to text
        _curdir = os.getcwd()
        os.chdir(_chdir)
        _cmdsql = "select type, name, urls from mmedia"
        if _usrs:
            _where = ' or '.join(list(map(lambda _usr: "name='%s'" % _usr, _usrs)))
            _cmdsql = _cmdsql + " where " + _where
        RALog.i('[%s] [%s] %s' % (_taskname, _tflag, _cmdsql))
        _sqlite = "sqlite3.exe -list tumblr_mmedias.db \"%s\" > %s" % (_cmdsql, _ftxt)
        _result = os.system(_sqlite)
        if _result:
            RALog.e('[%s] [%s] %s' % (_taskname, _tflag, _sqlite))
        os.chdir(_curdir)

        # trans text to list
        RALog.i('[%s] [%s] txt: %s' % (_taskname, _tflag, _ftxt))
        _srcstr = CFile.read_string(_ftxt)
        if not _srcstr:
            return
        _srcstr = _srcstr.replace(",\n\n", ",").replace(",\n", ",")
        CFile.write_string(_ftxt, _srcstr)

        # trans list to uris/urls
        _uris = list()
        _htps = list()

        _file = open(_ftxt)
        while True:
            _lines = _file.readlines(10000 * 100)
            if not _lines:
                break
            for _line in _lines:
                _cells = _line.strip('\n').split('|')
                _type = _cells[0]
                _name = _cells[1]
                _urls = _cells[2].split(',')
                for _url in _urls:
                    _filename = os.path.basename(_url)
                    if os.path.exists(os.path.join(_droot, _filename)) \
                            or os.path.exists(os.path.join(_troot, _type, _name, _filename)):
                        pass
                    else:
                        _uris.append(_url.strip('\n') + '\n out=' + _type + '\\' + _name + '\\' + _filename)
                        _htps.append(_url.strip('\n'))
        _file.close()

        CFile.write_list(_furi, _uris)
        RALog.i('[%s] [%s] uri: %s' % (_taskname, _tflag, _furi))

        CFile.write_list(_furl, _htps)
        RALog.i('[%s] [%s] url: %s' % (_taskname, _tflag, _furl))

        return

    @classmethod
    def _task_discover(cls, _taskname, args, _blogitems):

        _blognames = cls.taskctrl.operator(
            'query', _blogitems if _blogitems else TumblrParser.blognamestoblogitems(
                cls.get_need_update_blognames(TumblrCommon.gblogname)),
            _taskname, 'blognames')

        if not _blognames:
            return

        # perpare update for new blogname
        cls._run_task_perpare(_taskname, args, _blognames)
        cls.taskctrl.operator('update', list(), _taskname, 'blognames')

        _fkeyurls = TumblrCommon.get_keyurl_paths()

        return

        _blognames = list(sorted(set([_blogitem['blogname'] for _blogitem in _blogitems if _blogitem])))
        if not _blognames:
            return
        TumblrCommon.gblogname = TumblrCommon.update_tumblr_blognames(_blognames)

        return
