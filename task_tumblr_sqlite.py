# -*- coding: utf-8 -*-
# encoding=utf-8
from collections import OrderedDict
from sqlite3worker import Sqlite3Worker
from task_tumblr_parser import TumblrParser
from task_sqlite_common import DataHelper, DataCondition, _concat_keys


class Sqlite:
    @classmethod
    def init(cls, _dbfile, _max_queue_size=1000):
        cls.sqlworker = Sqlite3Worker(_dbfile, max_queue_size=_max_queue_size)

    @classmethod
    def uninit(cls):
        cls.sqlworker.close()

    @classmethod
    def _is_table_exist(cls, _table_name):
        _table_name = cls.fixed_table_name(_table_name)
        _condition = DataCondition(type='table', name=_table_name).toString()
        _sql = DataHelper().table('sqlite_master').select('count(*)').where(_condition).sql
        _sql = _sql.replace('[count(*)]', 'count(*)')
        _result = cls.execute(_sql)
        return _result[0][0] != 0

    @classmethod
    def is_table_exist(cls, _table_name):
        _table_name = cls.fixed_table_name(_table_name)
        _condition = DataCondition(type='table', name=_table_name).toString()
        _sqlhelper = DataHelper()
        _sql = _sqlhelper.table('sqlite_master').select('count(*)').where(_condition).sql
        _result = cls.execute(_sql)
        return _result[0][0] != 0

    @classmethod
    def select_columns(cls, _table):
        _sqlformat = 'PRAGMA table_info({0});'
        _sql = _sqlformat.format(_table)
        _result = cls.execute(_sql)
        return _result

    @classmethod
    def drop_table(cls, _table):
        _sqlformat = 'DROP TABLE {0};'
        _sql = _sqlformat.format(_table)
        _result = cls.execute(_sql)
        return _result

    @classmethod
    def execute(cls, _sql):
        return cls.sqlworker.execute(_sql)

    @classmethod
    def execute_with_columns(cls, _sql, _columns, _primary=''):
        _result = cls.sqlworker.execute(_sql)
        if isinstance(_result, str):
            return None
        elif _result and _columns:
            if _primary and _primary in _columns:
                _result = list(map(lambda __row: dict(zip(_columns, list(__row))), _result))
                _retder = OrderedDict()
                for _row in _result:
                    _retder[_row[_primary]] = dict()
                    for _column in _columns:
                        if _column != _primary:
                            _retder[_row[_primary]][_column] = _row[_column]
                return _retder
            else:
                _result = list(map(lambda __row: dict(zip(_columns, list(__row))), _result))
                return _result
        else:
            return None

    @classmethod
    def select(cls, _tables, _columns, _condition):
        _sql = cls.build_select(
            _tables if isinstance(_tables, str) else _concat_keys(_tables),
            _columns if isinstance(_columns, str) else _concat_keys(_columns),
            _condition)
        _result = cls.execute(_sql)
        if isinstance(_result, str):
            return None
        elif _result:
            _result = list(map(lambda _row: dict(zip(_columns, list(_row))), _result))
            return _result
        else:
            return list([None])

    @classmethod
    def create(cls, _table_name, _fields, _primary=''):
        if not _fields:
            return None
        _sql = cls.build_create(_table_name, _fields, _primary)
        _result = cls.sqlworker.execute(_sql)
        return _result

    @classmethod
    def insert(cls, _table_name, _fields):
        if not _fields:
            return None
        _sql = cls.build_insert(_table_name, _fields)
        _result = cls.sqlworker.execute(_sql)
        return _result

    @classmethod
    def replace(cls, _table_name, _fields):
        if not _fields:
            return None
        _sql = cls.build_replace(_table_name, _fields)
        _result = cls.sqlworker.execute(_sql)
        return _result

    @classmethod
    def build_create(cls, _table_name, _fields, _primary=''):
        if _primary:
            _sqlformat = 'CREATE TABLE IF NOT EXISTS {0} ({1}, PRIMARY KEY ({2}));'
            _sql = _sqlformat.format(
                _table_name,
                ', '.join(map(cls.build_type, _fields.items())), _primary)
        else:
            _sqlformat = 'CREATE TABLE IF NOT EXISTS {0} ({1});'
            _sql = _sqlformat.format(
                _table_name,
                ', '.join(map(cls.build_type, _fields.items())))
        return _sql

    @classmethod
    def build_select(cls, _tables, _columns, _condition=''):
        if _condition:
            _sqlformat = 'SELECT {0} FROM {1} WHERE {2};'
            _sql = _sqlformat.format(_columns, _tables, _condition)
        else:
            _sqlformat = 'SELECT {0} FROM {1};'
            _sql = _sqlformat.format(_columns, _tables)
        return _sql

    @classmethod
    def build_insert(cls, _table_name, _fields):
        _sqlformat = 'INSERT INTO {0} ({1}) VALUES ({2});'
        _sql = _sqlformat.format(
            _table_name,
            ', '.join(_fields),
            ', '.join(map(cls.build_value, _fields.items())))
        return _sql

    @classmethod
    def build_replace(cls, _table_name, _fields):
        _sqlformat = 'REPLACE INTO {0} ({1}) VALUES ({2});'
        _sql = _sqlformat.format(
            _table_name,
            ', '.join(_fields),
            ', '.join(map(cls.build_value, _fields.items())))
        return _sql

    @staticmethod
    def build_type(_fielditem):
        if isinstance(_fielditem[1], str):
            _type = 'TEXT'
        elif isinstance(_fielditem[1], bool):
            _type = 'BOOLEAN'
        elif isinstance(_fielditem[1], int):
            _type = 'INTEGER'
        else:
            raise
        return '%s %s' % (_fielditem[0], _type)

    @staticmethod
    def build_value(_fielditem):
        _value = _fielditem[1]
        if isinstance(_value, str):
            return '\'' + _value + '\''
        elif isinstance(_value, bool):
            return str(1 if _value else 0)
        else:
            return str(_value)

    @staticmethod
    def fixed_table_name(_tname):
        if _tname.startswith('sqlite_') or _tname.startswith('a_') or _tname.startswith('t_'):
            return _tname
        elif _tname == 'tumblr_blog' or _tname == 'tumblr_media':
            return '%s_%s' % ('a', _tname.replace('-', '_'))
        else:
            return '%s_%s' % ('t', _tname.replace('-', '_'))


class MMedia:
    BLOGNAME = 0
    TYPE = 1
    NOTECOUNT = 2
    REMOTEGMT = 3
    MEDIAURL = 4


class TumblrSqlite(Sqlite):
    blogcolumns = list()
    postcolumns = list()
    mkeycolumns = list()

    @classmethod
    def init_tumblr(cls, _tdatapath, _tmaxqsize):
        cls.init(_tdatapath, _max_queue_size=_tmaxqsize)

        _blogcolumns = list(TumblrParser.build_blog().keys())
        _postcolumns = list(TumblrParser.build_post().keys())
        _mkeycolumns = list(TumblrParser.build_media_key().keys())
        cls.init_tumblr_columns(_blogcolumns, _postcolumns, _mkeycolumns)

        cls.create(TumblrSqlite.fixed_table_name('tumblr_blog'), TumblrParser.build_blog(), 'blogname')

        return

    @classmethod
    def init_tumblr_columns(cls, _blogcolumns, _postcolumns, _mkeycolumns):
        cls.blogcolumns = _blogcolumns
        cls.postcolumns = _postcolumns
        cls.mkeycolumns = _mkeycolumns

    @classmethod
    def select_blog_info(cls, _blogname):
        _tables = list([cls.fixed_table_name('tumblr_blog')])
        _condition = DataCondition(blogname=_blogname).toString()
        _columns = list(cls.blogcolumns)
        _sql = DataHelper().table(_tables).select(_columns).where(_condition).sql
        _bloginfo = cls.execute_with_columns(_sql, _columns)
        if not _bloginfo:
            return None
        return _bloginfo[0]

    @classmethod
    def select_blogs(
            cls, _columns=list(['blogname']),
            _conditions=DataCondition(("<>", "AND"), False, status=200).toString(),
            _suffix='', _primary=''):
        _tables = list([cls.fixed_table_name('tumblr_blog')])
        _sql = DataHelper().table(_tables).select(_columns).where(_conditions).sql + _suffix
        _bloginfo = cls.execute_with_columns(_sql, _columns, _primary=_primary)
        return _bloginfo

    @classmethod
    def select_all_blogs(
            cls, _columns=list(['blogname']),
            _conditions=DataCondition(("<>", "AND"), False, status=999).toString(),
            _suffix='', _primary=''):
        _tables = list([cls.fixed_table_name('tumblr_blog')])
        _sql = DataHelper().table(_tables).select(_columns).where(_conditions).sql + _suffix
        _bloginfo = cls.execute_with_columns(_sql, _columns, _primary=_primary)
        return _bloginfo

    @staticmethod
    def converpost(_post):
        return [_post['blog_name'], _post['type'],
                _post['note_count'], _post['remote_gmt'], _post['media_url'].split('\n')]

    @classmethod
    def select_media(cls, _blogname, _pagesize=0, _pageinx=0, _types=list(['photo', 'video%'])):
        _tables = list([cls.fixed_table_name(_blogname)])
        _columns = list(['blog_name', 'media_key', 'type', 'note_count', 'remote_gmt', 'media_url'])
        if _types:
            _condition = list(map(lambda _type: DataCondition(('LIKE', 'AND'), type=_type), _types))
        else:
            _condition = ''
        _sql = DataHelper().table(_tables).select(_columns).where(_condition, "OR").sql
        if _pagesize:
            _sql = _sql + ' LIMIT {0} OFFSET {0}*{1}'.format(_pagesize, _pageinx)
        _posts = cls.execute_with_columns(_sql, _columns)
        if not _posts:
            return dict()
        _mkdict = dict()
        for _post in _posts:
            if not _post['media_key']:
                continue
            _mkdict[_post['media_key']] = cls.converpost(_post)
        return _mkdict

    @classmethod
    def select_blog_posts(cls, _blognames, _columns='', _conditions=list(),
                          _types=list(['photo', 'video%']), _suffix=''):
        if isinstance(_blognames, list):
            _tables = list(map(lambda _blogname: cls.fixed_table_name(_blogname), _blognames))
        else:
            _tables = cls.fixed_table_name(_blognames)
        if not _columns:
            _columns = cls.postcolumns
        if _types:
            if isinstance(_conditions, tuple):
                _conditions = _conditions[0]
            _conditions.extend(list(map(lambda _type: DataCondition(('LIKE', 'AND'), type=_type), _types)))
        # _con = 'WHERE ([remote_gmt] >= \'2018-01-01\') AND (([type] LIKE \'photo\') OR ([type] LIKE \'video%\'))'
        _con = 'WHERE (([type] LIKE \'photo\') OR ([type] LIKE \'video%\'))'
        _sql = DataHelper().table(_tables).select(_columns).sql + ' ' + _con + ' ' + _suffix
        _blogposts = cls.execute_with_columns(_sql, _columns)
        return _blogposts
