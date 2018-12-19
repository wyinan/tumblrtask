# -*- coding: utf-8 -*-
# encoding=utf-8
import os
import sys
import json
import time
import codecs
import pickle
import shutil
import winreg
import requests
import pandas as pd
import urllib.request
from task_common import RALog
from selenium import webdriver
from datetime import datetime, timedelta
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.action_chains import ActionChains


class CWorker:
    # Worker
    gworkerdir = 'G:\\tumblr_worker\\'
    gxunleidir = os.path.join(gworkerdir, 'xunlei')
    gspiderdir = os.path.join(gworkerdir, 'spider')
    gariadir = os.path.join(gspiderdir, '.aria')
    gcacherdir = os.path.join(gworkerdir, 'spider', '.cacher')
    gtumblrlocaldir = os.path.join(gworkerdir, 'tumblr')

    gttagsdir = os.path.join(gworkerdir, 'tumblrtags')

    # Target
    gtargetdir = 'G:\\AV\\'
    gtumblrdir = os.path.join(gtargetdir, 'tumblr')
    gxtubedir = os.path.join(gtargetdir, 'xtube')

    @classmethod
    def init(cls):
        CFile.mkdirs(cls.gxunleidir)
        CFile.mkdirs(cls.gcacherdir)
        CFile.mkdirs(cls.gariadir)
        CFile.mkdirs(os.path.join(cls.gtumblrlocaldir, 'photo'))
        CFile.mkdirs(os.path.join(cls.gtumblrlocaldir, 'video'))

        CFile.mkdirs(cls.gttagsdir)

        CFile.mkdirs(os.path.join(cls.gtumblrdir, 'photo'))
        CFile.mkdirs(os.path.join(cls.gtumblrdir, 'video'))
        CFile.mkdirs(cls.gxtubedir)

    @classmethod
    def spider_control(cls, _conf_flag, _conf_operate, _conf_value):
        _conf_path = os.path.join(cls.gspiderdir, 'spider_' + _conf_flag + '.conf')
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


class CNetWork:
    gwebbrowser = 0
    gproxy = dict()

    @classmethod
    def init(cls, _worker_dir, _spider_dir):
        cls.gchromedriver = os.path.join(_worker_dir, 'chromedriver.exe')
        _proxypath = os.path.join(_spider_dir, 'spider_proxy.json')
        cls._load_proxy(_proxypath)
        return

    @classmethod
    def uninit(cls):
        if cls.gwebbrowser:
            cls.gwebbrowser.close()
            cls.gwebbrowser = 0

    @classmethod
    def _load_proxy(cls, _proxypath):
        _proxy_conf = dict()
        if os.path.exists(_proxypath) and os.path.getsize(_proxypath):
            with open(_proxypath, 'r') as _proxy_json:
                _proxy_conf = json.load(_proxy_json)
                _proxy_json.close()
        else:
            _proxy_conf['enable'] = 0
            _proxy_conf['proxy'] = dict()
            _proxy_conf['proxy']['http'] = ''
            _proxy_conf['proxy']['https'] = ''
            with open(_proxypath, 'w+') as _proxy_json:
                _proxy_json.write(json.dumps(_proxy_conf, ensure_ascii=False, indent=4))
                _proxy_json.close()
        cls.gproxy = {}
        if not _proxy_conf:
            return
        if 'enable' in _proxy_conf and 'proxy' in _proxy_conf:
            if _proxy_conf['enable'] != 0:
                cls.gproxy = _proxy_conf['proxy']
            return
        return

    @classmethod
    def get_tumblr_xml(cls, _url):
        try:
            if not cls.gwebbrowser:
                cls.gwebbrowser = webdriver.Chrome(executable_path=cls.gchromedriver)
            cls.gwebbrowser.get('view-source:' + _url)
            _content = cls.gwebbrowser.find_element_by_tag_name('body').text
            _content = bytes(_content, encoding='utf8')
        except (WebDriverException, TimeoutException) as err:
            RALog.e('%s %s' % (_url, str(err)))
            return False, '', b''
        else:
            return True, '', _content

    @classmethod
    def get_tumblr_html(cls, _url):
        try:
            if not cls.gwebbrowser:
                cls.gwebbrowser = webdriver.Chrome(executable_path=cls.gchromedriver)
            cls.gwebbrowser.get(_url)
            _content = cls.gwebbrowser.page_source
            _content = bytes(_content, encoding='utf8')
        except (WebDriverException, TimeoutException) as err:
            RALog.e('%s %s' % (_url, str(err)))
            return False, '', b''
        else:
            return True, '', _content

    @classmethod
    def get_xtube_xml(cls, _url):
        if not cls.gwebbrowser:
            cls.gwebbrowser = webdriver.Chrome(executable_path=cls.gchromedriver)
        cls.gwebbrowser.get('view-source:' + _url)
        _content = cls.gwebbrowser.page_source
        _content = bytes(_content, encoding='utf8')
        return _content

    @classmethod
    def get_html_text(cls, _url):
        r = requests.get(_url, timeout=60)
        if r.status_code != 200:
            RALog.e('%d %s' % (r.status_code, _url))
            return False, '', ''
        else:
            return True, '', bytes(r.text, encoding='utf8')

    @classmethod
    def download_file(cls, _url, _target):
        try:
            urllib.request.urlretrieve(_url, _target)
        except IOError as err:
            RALog.d(str(err))
            return False
        except Exception as err:
            RALog.d(str(err))
            return False
        else:
            return True

    @staticmethod
    def get_local_proxy_server():
        xpath = 'Software\Microsoft\Windows\CurrentVersion\Internet Settings'
        try:
            key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, xpath, 0, winreg.KEY_READ)
            _pxy_enable, _type = winreg.QueryValueEx(key, 'ProxyEnable')
            _pxy_server, _type = winreg.QueryValueEx(key, 'ProxyServer')
        except TypeError as e:
            _pxy_enable = -1
            _pxy_server = ''
            RALog.e('get proxy: %s' % str(e))
        finally:
            return _pxy_enable, _pxy_server

    @staticmethod
    def get_proxy_server_str():
        _pxy_enable, _pxy_server = CNetWork.get_local_proxy_server()
        if _pxy_enable != 1:
            return ''
        else:
            return 'http://%s' % _pxy_server


class CNetWorkArrary:
    _webbrowser = 0

    def get_tumblr_xml(self, _url):
        if not self._webbrowser:
            self._webbrowser = webdriver.Chrome(executable_path=CNetWork.gchromedriver)
        self._webbrowser.get('view-source:' + _url)
        _content = self._webbrowser.find_element_by_tag_name('body').text
        _content = bytes(_content, encoding='utf8')
        return True, '', _content

    def uninit(self):
        if self._webbrowser:
            self._webbrowser.close()
            self._webbrowser = 0


class CProgressBar:
    def __init__(self, _range, _start=0):
        self.range = _range
        self.start = _start
        self.lastpos = -1

    def go(self, _item=': ', _breout=True, _bline=False):
        _position = int(self.start * 101 / self.range)
        _step = int(_position % 101)
        self.start = self.start + 1
        if not _breout and _step == self.lastpos:
            return
        self.lastpos = _step
        sys.stdout.write('\r')
        sys.stdout.write('%s%s%% |%s' % (_item, _step, _step * '#'))
        if _bline:
            sys.stdout.write('\n')
        sys.stdout.flush()


class CUtil:
    @staticmethod
    def timestamp2string(_timestamp, _timeformat='%Y-%m-%d %H:%M:%S'):
        return time.strftime(_timeformat, time.localtime(_timestamp))

    @staticmethod
    def timestamp2stringf(_timestamp, _timeformat='%Y-%m-%d %H:%M:%S.%f'):
        return time.strftime(_timeformat, time.localtime(_timestamp))

    @staticmethod
    def string2timestamp(_timestring):
        try:
            d = datetime.datetime.strptime(_timestring, "%Y-%m-%d %H:%M:%S.%f")
            t = d.timetuple()
            timestamp = int(time.mktime(t))
            timestamp = float(str(timestamp) + str("%06d" % d.microsecond)) / 1000000
            return timestamp
        except ValueError as e:
            d = datetime.datetime.strptime(_timestring, "%Y-%m-%d %H:%M:%S")
            t = d.timetuple()
            timestamp = int(time.mktime(t))
            timestamp = float(str(timestamp) + str("%06d" % d.microsecond)) / 1000000
            return timestamp

    @staticmethod
    def isearly(_timestamp, _time, _timedelta):
        _timeformat = '%Y-%m-%d %H:%M:%S'
        _tstrlocal = time.strftime(_timeformat, time.localtime(_timestamp))
        _tstrearliest = (_time + _timedelta).strftime(_timeformat)
        return _tstrlocal < _tstrearliest

    @staticmethod
    def placeholder(_index, _count, _phch='0'):
        _sindex = '%s%d' % (''.join([_phch] * (len(str(_count)) - len(str(_index + 1)))), _index + 1)
        return '%s/%d' % (_sindex, _count)


class CFile:
    @staticmethod
    def mkdirs(_dirpath):
        try:
            os.makedirs(_dirpath, 0o777, True)
        except OSError as err:
            RALog.e('MKDir \'%s\' %s' % (_dirpath, str(err)))
        finally:
            return

    @staticmethod
    def write_json(_file, _dict, _sort_keys=True):
        if not _dict:
            return _dict
        with open(_file, 'w+', encoding='utf-8') as _file_json:
            _file_json.write(json.dumps(_dict, ensure_ascii=False, indent=4, sort_keys=_sort_keys))
            _file_json.close()
        return _dict

    @staticmethod
    def read_json(_file):
        _dict = dict()
        if os.path.exists(_file) and os.path.getsize(_file) > 2:
            with open(_file, 'r', encoding='utf-8') as _file_json:
                try:
                    _dict = json.loads(_file_json.read())
                except UnicodeDecodeError as err:
                    RALog.e('read json \'%s\' %s' % (_file, str(err)))
                _file_json.close()
        return _dict

    @staticmethod
    def write_pickle(_file, _pdata):
        if not _pdata:
            return None
        with open(_file, 'wb') as _file_pickle:
            pickle.dump(_pdata, _file_pickle)
            _file_pickle.close()
        return _pdata

    @staticmethod
    def read_pickle(_file):
        if not os.path.exists(_file):
            return dict()
        try:
            with open(_file, 'rb') as _file_pickle:
                _pdata = pickle.load(_file_pickle)
                _file_pickle.close()
        except EOFError as err:
            print('%s, %s' % (_file, str(err)))
            return None
        except pickle.UnpicklingError as err:
            print('%s, %s' % (_file, str(err)))
            return None
        return _pdata

    @staticmethod
    def write_list(_file, _sites):
        if not _sites:
            return _sites
        with open(_file, 'w+') as _file_txt:
            _file_txt.write('\n'.join(list(sorted(set(_sites)))))
            _file_txt.close()
        return _sites

    @staticmethod
    def remove_empty_dir(_dir):
        if os.path.isdir(_dir):
            for _file in os.listdir(_dir):
                CFile.remove_empty_dir(os.path.join(_dir, _file))
            if not os.listdir(_dir):
                os.rmdir(_dir)
                RALog.i('[Remove]%s.' % _dir)

    @staticmethod
    def remove_empty_dir_simple(_dir):
        try:
            os.rmdir(_dir)
        except OSError:
            pass
        else:
            RALog.i('[Remove]%s.' % _dir)

    @staticmethod
    def read_list_easy(_file):
        _list = list()
        if not os.path.exists(_file) or not os.path.getsize(_file):
            return _list
        with open(_file, 'r') as _file_json:
            _strls = _file_json.read()
            _list = json.loads(_strls)
            _file_json.close()
        return _list

    @staticmethod
    def write_list_easy(_file, _list):
        if _list:
            _list = list(sorted(set(_list)))
            with open(_file, 'w+') as _file_list:
                _file_list.write(str(_list))
                _file_list.close()
        return _list

    @staticmethod
    def read_list(_file):
        _list = list()
        if not os.path.exists(_file) or not os.path.getsize(_file):
            return _list
        if not os.path.getsize(_file):
            return _list
        with open(_file, "r") as _file_txt:
            _list = _file_txt.read().strip().strip('\n').split('\n')
            _file_txt.close()
        return _list

    @staticmethod
    def write_list(_file, _list, _mode='c'):
        if not _list:
            return _list
        if _mode == 'u':
            _local_list = CFile.read_list(_file)
            _local_list.extend(_list)
            _list = _local_list
        elif _mode == 'a' and os.path.exists(_file):
            _list.extend(CFile.read_list(_file))
        if _list:
            _list = list(sorted(set(_list)))
            with open(_file, 'w+') as _file_list:
                _file_list.write('\n'.join(_list))
                _file_list.close()
        return _list

    @staticmethod
    def read_string(_file, _defstr=''):
        if not os.path.exists(_file) or not os.path.getsize(_file):
            return _defstr
        with open(_file, 'r', encoding='utf-8') as _file_txt:
            _defstr = _file_txt.read()
            _file_txt.close()
        return _defstr

    @staticmethod
    def write_string(_file, _str, _mode='w+'):
        with open(_file, _mode, encoding='utf-8') as _file_txt:
            _file_txt.write(str(_str))
            _file_txt.close()
        return

    @staticmethod
    def read_dict(_file):
        _dict = dict()
        if not os.path.exists(_file):
            return _dict
        if not os.path.getsize(_file):
            os.remove(_file)
            return _dict
        with open(_file, 'r') as _file_json:
            _dict = json.load(_file_json)
            _file_json.close()
        if _dict:
            return _dict
        else:
            os.remove(_file)
            return _dict

    @staticmethod
    def write_dict_utf(_file, _dict, _mode='c'):
        if _mode == 'u':
            _local_dict = CFile.read_dict(_file)
            _local_dict.update(_dict)
            _dict = _local_dict
        elif _mode == 'a':
            _dict.update(CFile.read_dict(_file))
        if _dict:
            _dict = dict(sorted(_dict.items(), key=lambda d: d[0]))
            with open(_file, 'w+', encoding='utf-8') as _file_json:
                _file_json.write(json.dumps(_dict, ensure_ascii=False, indent=4))
                _file_json.close()
        return _dict

    @staticmethod
    def read_dict_utf(_file):
        _dict = dict()
        if not os.path.exists(_file):
            return _dict
        if not os.path.getsize(_file):
            os.remove(_file)
            return _dict
        with open(_file, 'r', encoding='utf-8') as _file_json:
            _dict = json.load(_file_json)
            _file_json.close()
        if _dict:
            return _dict
        else:
            os.remove(_file)
            return _dict

    @staticmethod
    def write_dict(_file, _dict, _mode='c'):
        if _mode == 'u':
            _local_dict = CFile.read_dict(_file)
            _local_dict.update(_dict)
            _dict = _local_dict
        elif _mode == 'a':
            _dict.update(CFile.read_dict(_file))
        if _dict:
            _dict = dict(sorted(_dict.items(), key=lambda d: d[0]))
            with open(_file, 'w+') as _file_json:
                _file_json.write(json.dumps(_dict, ensure_ascii=False, indent=4))
                _file_json.close()
        return _dict

    @staticmethod
    def read_data_frame(_file, _columns, _types):
        if not os.path.exists(_file) or os.path.getsize(_file) == 2:
            _df = pd.DataFrame(columns=_columns)
            for _index in range(len(_columns)):
                _df[[_columns[_index]]] = \
                    _df[[_columns[_index]]].astype(_types[_index])
            return _df
        _dflocal = pd.read_csv(_file, encoding='utf_8', dtype='str')
        for _index in range(len(_columns)):
            _dflocal[[_columns[_index]]] = \
                _dflocal[[_columns[_index]]].astype(_types[_index])
        return _dflocal

    @staticmethod
    def write_data_frame(_file, _df, _columns, _types, _sort, _ascending=False, _subset=list(), _keep='last'):
        for _index in range(len(_columns)):
            _df[[_columns[_index]]] = \
                _df[[_columns[_index]]].astype(_types[_index])
        if _df.empty:
            return 0
        _df = _df.drop_duplicates()
        _df = _df.sort_values(by=_sort, ascending=_ascending)
        if _subset:
            _df_repeat = _df[_df.duplicated(_subset, keep=_keep)]
            if not _df_repeat.empty:
                _df = _df.drop(_df_repeat.index)
        _df.to_csv(_file, encoding='utf_8', index=False, mode='w')
        return len(_df)

    @staticmethod
    def write_data_frame_list(_file, _list, _columns, _types, _sort, _ascending=True, _mode='w'):
        _df = pd.DataFrame(_list, columns=_columns)
        for _index in range(len(_columns)):
            _df[[_columns[_index]]] = \
                _df[[_columns[_index]]].astype(_types[_index])
        if _mode == 'a+':
            _dflocal = CFile.read_data_frame(_file, _columns, _types)
            if not _dflocal.empty:
                _df = _df.append(_dflocal)
        _df = _df.drop_duplicates()
        _df = _df.sort_values(by=_sort, ascending=_ascending)
        _df.to_csv(_file, encoding='utf_8', index=False)
        return _df

    @staticmethod
    def write_data_frame_speed(_file, _list, _columns, _mode='a+'):
        _df = pd.DataFrame(_list, columns=_columns)
        if not os.path.exists(_file) or not os.path.isfile(_file):
            _df.to_csv(_file, encoding='utf_8', index=False, mode=_mode)
        else:
            _df = pd.DataFrame(_list)
            _df.to_csv(_file, encoding='utf_8', columns=_columns, index=False, header=False, mode=_mode)
        return len(_df)

    @staticmethod
    def sort_data_frame_speed(_file, _columns, _types, _sort, _ascending=False, _subset=list(), _keep='last'):
        _df = CFile.read_data_frame(_file, _columns, _types)
        if _df.empty:
            return 0
        _df = _df.drop_duplicates()
        _df = _df.sort_values(by=_sort, ascending=_ascending)
        if _subset:
            _df_repeat = _df[_df.duplicated(_subset, keep=_keep)]
            if not _df_repeat.empty:
                _df = _df.drop(_df_repeat.index)
        _df.to_csv(_file, encoding='utf_8', index=False, mode='w')
        return len(_df)

    @staticmethod
    def move_media(_site, _type, _target_path, _file_name, _file_path):
        RALog.i('[%s][%s][Move-None][%s]%s' % (_site, _type, _file_name, _file_path))
        return False
        if _target_path == _file_path:
            RALog.w('[%s][%s][Move][%s]Same %s' % (_site, _type, _file_name, _file_path))
            return False
        _cache_path = _target_path + '.cache'
        if os.path.exists(_cache_path):
            os.remove(_cache_path)
        if os.path.exists(_file_path) and os.path.isfile(_file_path):
            if os.path.exists(_target_path):
                _sz_src = os.path.getsize(_file_path)
                _sz_dst = os.path.getsize(_target_path)
                RALog.i('[%s][%s][Move][%s]src:%d dst:%d %s -> %s' %
                        (_site, _type, _file_name, _sz_src, _sz_dst, _file_path, _target_path))
                if _sz_src <= _sz_dst:
                    os.remove(_file_path)
                    RALog.i('[%s][%s][Move-Ignore][%s]%s' % (_site, _type, _file_name, _file_path))
                    return False
            shutil.move(_file_path, _cache_path)
            if os.path.exists(_target_path):
                os.remove(_target_path)
            os.rename(_cache_path, _target_path)
            return True
        else:
            return False
