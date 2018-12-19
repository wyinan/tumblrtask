# -*- coding: utf-8 -*-
# encoding=utf-8
import os
import re
import gc
import sys
import html
import time
import json
import string
import requests
from task_common import RALog
from datetime import datetime
from task_spider_common import CFile, CUtil
from task_tumblr_common import TumblrCommon, TumblrControl
from task_tumblr_sqlite import MMedia, TumblrSqlite
from task_tumblr_parser import TumblrParser
from task_sqlite_common import DataHelper, DataCondition


class TumblrAPI(object):
    API_KEY = 'zLgPh6LeV7DyczfPALkTEfr8rOgzcYAY8TzAlabVIYrgpATPON'

    gheaders = {
        "Accept": "text/html,application/xhtml+xml,application/xml; "
                  "q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "text/html",
        "Accept-Language": "en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4,zh-TW;q=0.2",
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": "https://api.tumblr.com/console//calls/blog/posts",
        "User-Agent": "Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/32.0.1700.77 Safari/537.36"
    }

    gsession = requests.session()
    gsession.headers.update(gheaders)

    @staticmethod
    def _request(_blogname, _target, _type, _params):
        _api_url = '/'.join(['https://api.tumblr.com/v2/blog',
                             _blogname, _target, _type])

        if _params:
            _params.setdefault('api_key', TumblrAPI.API_KEY)
        else:
            _params = dict({'api_key': TumblrAPI.API_KEY})

        while True:
            try:
                _response = TumblrAPI.gsession.get(_api_url, params=_params, timeout=60)
                _jsondata = _response.json()
                break
            except KeyboardInterrupt as err:
                RALog.i('%s, %s.' % (_api_url, str(err)))
                sys.exit()
            except (requests.exceptions.ProxyError, requests.exceptions.ReadTimeout) as err:
                RALog.i('%s, %s.' % (err.request.url, str(err)))
                time.sleep(5)
            except requests.exceptions.ConnectTimeout as err:
                RALog.i('%s, %s.' % (err.request.url, str(err)))
                time.sleep(5)
            except json.JSONDecodeError as err:
                RALog.e('%s, %d, %s.' % (_response.url, _response.status_code, str(err)))
                return None
            except Exception as err:
                RALog.e('[E]%s, %s.' % (_api_url, str(err)))
                time.sleep(5)

        return _jsondata

    def _blog_info(self, _blogname):
        _params = {
            'filter': 'text'
        }

        _info = self._request(_blogname, 'info', '', _params)
        try:
            if isinstance(_info['response'], list):
                _info['response'] = dict()
            _bloginfo = _info['response'].setdefault('blog', dict())
        except AttributeError as err:
            RALog.e('%s, %s.' % (_blogname, str(err)))

        _bloginfo.setdefault('total_posts', -1)
        _bloginfo.update(_info['meta'])
        if _bloginfo['status'] == 200:
            _bloginfo['ask_page_title'] = self._conver_string(_bloginfo['ask_page_title'])
            _bloginfo['blogname'] = self._conver_string(_bloginfo['name'])
            del (_bloginfo['name'])
            _bloginfo['title'] = self._conver_string(_bloginfo['title'])
            _bloginfo['description'] = self._conver_string(_bloginfo['description'])
            _bloginfo['updated'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(_bloginfo['updated']))
            if 'submission_page_title' in _bloginfo:
                _bloginfo['submission_page_title'] = self._conver_string(_bloginfo['submission_page_title'])
            if 'submission_terms' in _bloginfo:
                del (_bloginfo['submission_terms'])
        else:
            _bloginfo['blogname'] = _blogname
        _bloginfo['update_live'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return _bloginfo

    def _posts_page(self, _blogname, _type='', _offset='', _tag='', _post_id=''):
        _params = {
            'offset': _offset,
            'tag': _tag,
            'id': _post_id,
            'limit': 50,
            'filter': 'text'
        }
        raw_data = self._request(_blogname, 'posts', _type, _params)
        return raw_data

    @staticmethod
    def _conver_string(_string):
        _string = html.unescape(_string)
        _string = re.sub(re.compile(r'</?\w+[^>]*>', re.S), '', _string)
        _string = re.sub('[\r\n\t]', ' ', _string)
        _string = _string.replace('  ', ' ')
        return _string.strip()


class TumblrUpdater(TumblrAPI):
    def __init__(self, _taskname, args, _blogitem, _blogflag):
        self.taskname = _taskname
        self.args = args
        self.blogitem = _blogitem
        if _blogitem['postid']:
            self.blogflag = \
                '[%s] [%s] [%s - %s]' % \
                (self.taskname, _blogflag, _blogitem['blogname'], str(_blogitem['postid']))
        else:
            self.blogflag = \
                '[%s] [%s] [%s]' % \
                (self.taskname, _blogflag, _blogitem['blogname'])
        self.sqlhelper = TumblrSqlite()
        self.taskctrl = TumblrControl()
        self.funcdict = dict({
            'update': self.run_tumblr_update,
            'discover': self.run_tumblr_discover
        })

        ''' stored '''
        self.mmdict = dict()

    def run_tumblr(self):
        if not self.taskctrl.isrunning():
            return
        if self.taskctrl.throperator(
                'query', False, self.taskname, self.blogitem['blogname'], 'completed'):
            return
        if self.funcdict.get(self.taskname):
            _func = self.funcdict[self.taskname]
            _func(self.blogitem)
        self.taskctrl.throperator(
            'update', True, self.taskname, self.blogitem['blogname'], 'completed')
        return

    def run_tumblr_update(self, _blogitem):
        if self.blogitem['postid']:
            self.run_tumblr_update_post(_blogitem)
        else:
            self.run_tumblr_update_blog(_blogitem)
        if self.mmdict:
            _fmmedia = TumblrCommon.get_mmedia_path(_blogname=_blogitem['blogname'])
            CFile.write_dict(_fmmedia, self.mmdict, _mode='a')
            gc.collect()
        return

    def run_tumblr_update_blog(self, _blogitem):
        _blogname = _blogitem['blogname']
        _bloginfo = self.get_blog_info(_blogname, online=self.args.update)
        if not _bloginfo:
            return
        if _bloginfo['status'] != 200 or (self.args.discover and self.args.update):
            self.set_blog_info(_bloginfo)
            return
        _table = TumblrSqlite().fixed_table_name(_blogname)
        TumblrSqlite().create(_table, TumblrParser.build_post(), 'id')
        self.update_posts(_bloginfo, _table)
        self.set_blog_info(_bloginfo)
        return

    @staticmethod
    def build_mmedia(_mkey, _mmedia, _usename=True):
        _tudir = TumblrCommon.tumblrdir
        _mfile = dict()
        _muri = dict()
        _fexts = set()
        for _url in _mmedia[MMedia.MEDIAURL]:
            try:
                _fname = os.path.split(_url)[1]
            except IndexError:
                _fname = ''
                pass
            if not _fname:
                RALog.e('Get file name failed %s, %s.' % (_url, str(_mmedia[0:-2])))
                continue
            try:
                _fext = os.path.splitext(_fname)[1]
            except IndexError:
                _fext = ''
                pass
            if not _fext:
                RALog.e('Get file extension failed %s, %s.' % (_url, str(_mmedia[0:-2])))
                continue
            _fexts.add(_fext)
            _mfile[_fname] = os.path.join(_tudir, '$targetdir', _fname)
            _muri[_fname] = ('%s\%s' % ('$targetdir', _fname))

        if not _mfile or not _muri or not _fexts:
            return None, None, None

        _values = {'targetdir': '', 'k': _mkey}
        _type = _mmedia[MMedia.TYPE].split(':')[0]
        if _type != 'photo':
            _clsdir = _type
        elif len(_mmedia[MMedia.MEDIAURL]) == 1:
            if '.gif' in _fexts:
                _clsdir = 'gif'
            else:
                _clsdir = 'photo'
        elif len(_fexts) == 1 and '.gif' in _fexts:
            _clsdir = 'gifs'
        else:
            _clsdir = 'photos'

        _blogname = _mmedia[MMedia.BLOGNAME] if _usename else '$blogname'
        _values['targetdir'] = os.path.join(_clsdir, _blogname)

        for _fname, _uri in _muri.items():
            _nuri = string.Template(_uri).substitute(_values)
            _muri[_fname] = _nuri
            _mfile[_fname] = string.Template(_mfile[_fname]).substitute(_values)

        _nmfile = dict()
        for _fname, _fpath in _mfile.items():
            _nfpath = string.Template(_fpath).substitute(_values)
            _nmfile[_fname] = _nfpath

        return _muri, _nmfile, _values['targetdir']

    def run_tumblr_update_post(self, _blogitem):
        _blogname = _blogitem['blogname']
        _postid = _blogitem['postid']
        _postpage = self._posts_page(_blogname, _type='', _offset='', _tag='', _post_id=_postid)
        if not _postpage:
            return
        elif _postpage['meta']['status'] != 200 or _postpage['meta']['msg'].lower() != 'ok':
            RALog.e('%s Postid %s Error %s' % (self.blogflag, _postid, _postpage['meta']['msg']))
            return
        elif not _postpage.get('response'):
            return
        _postinfo = _postpage['response']
        if not _postinfo.get('posts'):
            return

        _posts = TumblrParser.parse_posts(_postinfo['posts'])
        for _post in _posts:
            if isinstance(_post, str):
                print(_post)
            else:
                _key = _post['media_key']
                if not _key:
                    continue
                _type = _post['type']
                if _key and _type in list(['photo', 'video:tumblr']):
                    _media = TumblrSqlite.converpost(_post)
                    _muri, _mfile, _targetdir = TumblrUpdater.build_mmedia(_key, _media)
                    self.mmdict[_key] = \
                        tuple([_key, _targetdir.split('\\')[1], _targetdir.split('\\')[0],
                               _media[MMedia.NOTECOUNT], _media[MMedia.REMOTEGMT],
                               ',\n'.join(_muri.values()), ',\n'.join(_media[MMedia.MEDIAURL]), False])
        return

    def get_blog_info_online(self, _blogname):
        _bloginfo = self._blog_info(_blogname)
        _bloginfo = TumblrParser.parse_blog(_bloginfo)
        return _bloginfo

    def get_blog_info_local(self, _blogname):
        _bloginfo = self.sqlhelper.select_blog_info(_blogname)
        if _bloginfo:
            _post_pages = _bloginfo['post_pages']
            if _post_pages:
                _process = _bloginfo['post_pages'].split('\n')[0]
                _postpages = _bloginfo['post_pages'][len(_process) + 1:]
                _bloginfo['post_pages'] = json.loads(_postpages)
                if _bloginfo['post_pages'].get('0'):
                    _bloginfo['local_posts'] = _bloginfo['post_pages']['0']
            else:
                _bloginfo['post_pages'] = {}
        return _bloginfo

    def get_blog_info(self, _blogname, online=False):
        if online:
            _webinfo = self.get_blog_info_online(_blogname)
        else:
            _webinfo = {}
        _localinfo = self.get_blog_info_local(_blogname)
        if not _webinfo:
            return _localinfo
        if not _localinfo:
            return _webinfo
        _webinfo['post_pages'] = _localinfo['post_pages']
        _webinfo['local_posts'] = _localinfo['local_posts']
        return _webinfo

    def set_blog_info(self, _bloginfo):
        if _bloginfo['status'] == 200:
            if _bloginfo['post_pages'].get('0'):
                _bloginfo['local_posts'] = _bloginfo['post_pages']['0']
            _bloginfo['post_pages'] = TumblrParser.calc_blog_posts(_bloginfo['total_posts'], _bloginfo['post_pages'])
            _bloginfo['title'] = _bloginfo['title'].replace('\'', '\'\'')
            _bloginfo['ask_page_title'] = _bloginfo['ask_page_title'].replace('\'', '\'\'')
            _bloginfo['submission_page_title'] = _bloginfo['submission_page_title'].replace('\'', '\'\'')
        else:
            _bloginfo['post_pages'] = ''
        if 'show_top_posts' in _bloginfo.keys():
            del _bloginfo['show_top_posts']
        self.sqlhelper.replace(TumblrSqlite.fixed_table_name('tumblr_blog'), _bloginfo)

    def set_post(self, _bloginfo, _postinfo, _table, _offset):
        _total = _postinfo['blog']['total_posts']
        _posts = TumblrParser.parse_posts(_postinfo['posts'], _total, _offset)
        for _post in _posts:
            if isinstance(_post, str):
                print(_post)
            else:
                self.sqlhelper.replace(_table, _post)
                _key = _post['media_key']
                if not _key:
                    continue
                _type = _post['type']
                if _key and _type in list(['photo', 'video:tumblr']):
                    _media = TumblrSqlite.converpost(_post)
                    _muri, _mfile, _targetdir = TumblrUpdater.build_mmedia(_key, _media)
                    self.mmdict[_key] = \
                        tuple([_key, _targetdir.split('\\')[1], _targetdir.split('\\')[0],
                               _media[MMedia.NOTECOUNT], _media[MMedia.REMOTEGMT],
                               ',\n'.join(_muri.values()), ',\n'.join(_media[MMedia.MEDIAURL]), False])

        _bloginfo['post_pages'][str(_offset)] = _total

        return

    def update_posts_offset(self, _bloginfo, _table, _offset):
        _postpage = self._posts_page(_bloginfo['blogname'], _type='', _offset=_offset, _tag='', _post_id='')
        if not _postpage:
            RALog.e('%s Offset %d Error' % (self.blogflag, _offset))
            return
        if _postpage['meta']['status'] == 200 and _postpage['meta']['msg'].lower() == 'ok':
            _postinfo = _postpage['response']
            self.set_post(_bloginfo, _postinfo, _table, _offset)
        else:
            RALog.e('%s Offset %d Error %s' % (self.blogflag, _offset, _postpage['meta']['msg']))
        return

    def update_posts(self, _bloginfo, _table):
        _posts_pages = _bloginfo['post_pages']
        _local_posts = _bloginfo['post_pages']['0'] if _bloginfo['post_pages'].get('0') else -1
        _total_posts = _bloginfo['total_posts']

        _need_max = sys.maxsize if self.args.number == -1 else self.args.number
        _blog_max = min(_total_posts, _need_max)
        _blog_need = _blog_max if _local_posts == -1 else max(0, _total_posts - _local_posts)
        if _blog_need == 0 and len(_bloginfo['post_pages']) == ((_blog_max / 50) + (_blog_max % 50)):
            return 0

        _offset = 0
        _updater = list()
        while _offset < _blog_max:
            if not _posts_pages.get(str(_offset)):
                _updater.append(_offset)
            elif _offset < _blog_need:
                if _posts_pages[str(_offset)] != _total_posts:
                    _updater.append(_offset)
            _offset += 50
        if 0 in _updater:
            _updater.remove(0)
            _updater.append(0)

        _page_count = len(_updater)
        if _page_count == 0:
            return -1

        _cacher = self.taskctrl.throperator(
            'query', dict({'index': -1, 'offset': -1, 'total': -1}),
            self.taskname, self.blogitem['blogname'], 'offset')
        if _total_posts == _cacher['total']:
            _updater = _updater[_cacher['index'] + 1:]

        for _index, _offset in enumerate(_updater):
            self.update_posts_offset(_bloginfo, _table, _offset)
            _percent = min(1.0, float((_index + 1) / _page_count)) * 100
            RALog.i('%s [%0.2f%%(%s)] Offset: %d' %
                    (self.blogflag, _percent, CUtil.placeholder(_index, _page_count), _offset))
            if (_index + 1) % 50 == 0:
                __bloginfo = _bloginfo.copy()
                self.set_blog_info(__bloginfo)
                self.taskctrl.throperator(
                    'update', dict({'index': _index, 'offset': _offset, 'total': _total_posts}),
                    self.taskname, self.blogitem['blogname'], 'offset')
        return 1

    def run_tumblr_discover(self, _blogitem):
        _blogname = _blogitem['blogname']
        _furi = TumblrCommon.get_keyurl_path(_blogname)
        RALog.i('%s %s' % (self.blogflag, _furi))

        if os.path.exists(_furi):
            return

        _typos = dict({
            'photo': {'fsum': lambda _sum: max(int(_sum * 0.2 + 0.5), 1),
                      'sqls': list()
                      },
            'video:tumblr': {'fsum': lambda _sum: max(int(_sum * 0.1 + 0.5), 1),
                             'sqls': list()
                            }
        })

        _sqlitems = list()

        _tables = TumblrSqlite().fixed_table_name(_blogname)
        _columns = list(['media_key', 'type', 'media_url'])
        _dhelp = DataHelper().table(_tables).select(_columns)

        _start = self.args.start
        _end = self.args.end
        _conditions = list()
        if _start:
            _conditions.append(DataCondition(('>', 'AND'), ingroup=False, remote_gmt=_start).toString())
        if _end:
            _conditions.append(DataCondition(('<', 'AND'), ingroup=False, remote_gmt=_end).toString())
        _cdate = ' AND '.join(_conditions) if _conditions else ''
        if _cdate:
            _ctype = ' OR '.join(list(map(lambda __type: '[type] LIKE \'%s\'' % __type, _typos.keys())))
            _condition = "((%s) AND (%s))" % (_cdate, _ctype)
            _sql = _dhelp.sql
            _sql += (" WHERE %s" % _condition)
            _sql += (" ORDER BY [%s] %s" % ('remote_gmt', 'DESC'))
            _limit = int(self.args.limit)
            _sqlitems.append({'sql': _sql, 'limit': max(0, _limit)})

        for _type, _typo in _typos.items():
            _sqlcnt = "SELECT count([id]) FROM [%s] WHERE ([type] LIKE '%s')" % (_tables, _type)
            _retcnt = self.sqlhelper.execute_with_columns(_sqlcnt, list(['count']))
            if not _retcnt:
                continue
            _tysum = _retcnt[0].setdefault('count', 0)
            if _tysum < 1:
                continue
            _sql = _dhelp.sql
            _sql += (" WHERE %s" % DataCondition(('LIKE', 'AND'), type=_type).toString())
            _sql += (" ORDER BY [%s] %s" % ('note_count', 'DESC'))
            _limit = _typo['fsum'](_tysum)
            try:
                _sqlitems.append({'sql': _sql, 'limit': max(0, _limit)})
            except SystemError as err:
                print(str(err))

        _keyurl = dict()
        for _sqlitem in _sqlitems:
            _sqlhead = _sqlitem['sql']
            _limit = _sqlitem['limit']

            _pagesize = 1000
            _offset = 0
            while True:
                _cursize = max(0, min(_pagesize, _limit - _offset)) if _limit else _pagesize
                if _cursize == 0:
                    break
                _sql = _sqlhead + ' ' + 'LIMIT {0} OFFSET {1}'.format(_cursize, _offset)
                _offset = _offset + _pagesize

                _posts = self.sqlhelper.execute_with_columns(_sql, _columns)
                if not _limit and not _posts:
                    break

                for _post in _posts:
                    _key = _post['media_key']
                    _urls = _post['media_url'].split('\n')
                    _keyurl[_key] = _urls

        CFile.write_json(_furi, _keyurl, _sort_keys=True)

        return