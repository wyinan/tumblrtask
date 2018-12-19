# -*- coding: utf-8 -*-
# encoding=utf-8
import os
import re
import time
import json
import urllib
from urllib.parse import urlparse
import numpy as np
from task_common import RALog
from task_spider_common import CFile


class TumblrParser:
    @staticmethod
    def build_media_key():
        _mkey = dict({
            'type': '',
            'media_key': '',
            'blog_name': '',
            'blog_root': ''
        })
        return _mkey

    @staticmethod
    def build_blog():
        _blog = dict({
            'blogname': '', 'title': '',
            'likes': 0, 'posts': 0, 'post_pages': '',
            'total_posts': 0, 'updated': '',
            'local_posts': -1, 'update_live': '',
            'is_adult': False, 'is_nsfw': True,
            'ask': True, 'ask_anon': True, 'ask_page_title': '',
            'reply_conditions': '', 'share_likes': True,
            'submission_page_title': '',
            'can_subscribe': False, 'subscribed': False, 'is_optout_ads': True,
            'x_tumblr_content_rating': '',
            'status': 200, 'msg': 'OK'
        })
        return _blog

    @classmethod
    def parse_blog(cls, blog):
        _blog = cls.build_blog()
        for _key in blog.keys():
            if _key in list(['description', 'url']):
                continue
            elif _key.endswith('title'):
                if _key == 'title':
                    _blog[_key] = \
                        '\n'.join(list([blog['title'], blog['url'], blog['description']]))
                else:
                    _blog[_key] = blog[_key]
                _blog[_key] = _blog[_key].replace('\'', '\'\'')
            else:
                _blog[_key] = blog[_key]
        _blog['post_pages'] = {}
        _blog.setdefault('local_posts', -1)
        _blog.setdefault('update_live', '')
        _blog.setdefault('uuid', '')
        _blog.pop('uuid')
        return _blog

    @classmethod
    def calc_blog_posts(cls, _total, _pages):
        _count = 50
        _chno = '-'
        _char = '|'

        if _total < 0:
            return ''

        _totall = int(_total / 50) + min(1, (_total % 50))
        if _pages and len(_pages) == int(_totall):
            _inls = [_char] * _count
        elif not _pages:
            _inls = [_chno] * _count
        else:
            _npls, _step = np.linspace(start=0.0, stop=_totall, num=_count, endpoint=False, dtype=float, retstep=True)
            _nkls = np.rint(_npls).tolist()
            _pgls = dict(zip(sorted(_pages.keys()), [_char] * len(_pages.keys())))
            _inls = list(map(lambda _index: _pgls.setdefault(int(_index), _chno), _nkls))

        _rate = '% 7.2f%%:[%s]' % ((len(_pages) / _totall) * 100 if _totall != 0 else 100.0, ''.join(_inls))
        _pages = dict(zip(list(map(lambda _page: int(_page), _pages.keys())), _pages.values()))
        _json = json.dumps(_pages, ensure_ascii=False, indent=4, sort_keys=True)

        return '\n'.join(list([_rate, _json]))

    @classmethod
    def parse_posts(cls, posts, total=-1, offset=-1):
        _posts = list()
        for post in posts:
            _posts.append(cls.parse_post(post, total, offset))
        return _posts

    @classmethod
    def parse_mkey(cls, post):
        _mkey = dict({
            'type': post['type'],
            'media_key': post['media_key'],
            'blog_name': post['blog_name'],
            'blog_root': post['blog_root']
        })
        if _mkey['media_key'] and _mkey['blog_name']:
            return _mkey
        else:
            return None

    @classmethod
    def parse_post(cls, post, total=-1, offset=-1):

        _ftype = {
            'answer': cls._parse_post_answer,
            'audio': cls._parse_post_audio,
            'chat': cls._parse_post_chat,
            'link': cls._parse_post_link,
            'photo': cls._parse_post_photo,
            'quote': cls._parse_post_quote,
            'text': cls._parse_post_text,
            'video': cls._parse_post_video,
            'unknown': cls._parse_post_unknown
        }

        try:
            _post = cls._parse_post(post, total, offset)
            _type = post['type']
            if _type not in _ftype.keys():
                _type = 'unknown'

            _post = _ftype[_type](_post, post)

            cls._parse_post_summary(_post, post)

        except Exception as err:
            _errinfo = 'post %s, %s.' % (post, str(err))
            return _errinfo

        return _post

    @staticmethod
    def build_post():
        _post = dict({
            'id': 0, 'local_time': '',
            'blog_name': '', 'blog_root': '', 'note_count': 0,
            'type': '', 'extension': '',
            'post_info': '',
            'media_url': '', 'media_key': '',
            'remote_gmt': '', 'total': -1, 'offset': -1, 'tags': ''
        })
        return _post

    @classmethod
    def _parse_post_unknown(cls, post, total=-1, offset=-1):
        RALog.e('%s, %s.' % (post['post_url'], post['type']))
        return None

    @classmethod
    def __parse_all_urls(cls, _post):
        _urls = dict()
        for _key, _value in _post.items():
            if not _key.endswith('url'):
                continue
            elif not isinstance(_value, str):
                continue
            if _key == 'short_url':
                continue
            elif _key == 'thumbnail_url':
                continue
            _urls[_key] = cls.__parse_url(_value)
        return _urls

    @classmethod
    def _parse_post_summary(cls, _post, post):
        post['post_url'] = 'https://%s.tumblr.com/post/%s' % (post['blog_name'], post['id'])
        _urls = cls.__parse_all_urls(post)
        post['total_url'] = json.dumps(_urls, ensure_ascii=False, indent=4)
        _info_list = list(['album', 'artist', 'track_name',
                           'slug', 'summary',
                           'caption', 'caption_abstract',
                           'publisher', 'post_author',
                           'feed_item', 'link_author', 'year', 'plays',
                           'title', 'source_title',
                           'post_author', 'question', 'asking_name', 'answer',
                           'description', 'description_abstract',
                           'source', 'text', 'body', 'dialogue', 'total_url'])
        _info_dict = cls.__possible_extends(post, _info_list)
        _info = json.dumps(_info_dict, ensure_ascii=False, indent=4)
        _post['post_info'] = '%s\n\n%s' % (post['post_url'], _info)
        _post['post_info'] = _post['post_info'].replace('\'', '\'\'')
        _post['media_url'] = _post['media_url'].replace('\'', '\'\'')
        _post['media_key'] = _post['media_key'].replace('\'', '\'\'')
        return _post

    @staticmethod
    def _parse_blog_root(post):
        if post.get('post_author'):
            _blog_root = post['post_author']
        elif post.get('is_root_item') and post['is_root_item']:
            _blog_root = post['blog_name']
        elif post.get('source_url'):
            _turl = re.search(
                r'(http://|https://|)(?P<hostname>.+\.tumblr.com/post/)',
                post['source_url'])
            if _turl:
                _blog_root = _turl.group('hostname') \
                    .replace('http://', '').replace('https://', '')
                _blog_root = _blog_root.split('.')[0]
            else:
                _blog_root = post['blog_name']
        else:
            _blog_root = post['blog_name']
        return _blog_root

    @classmethod
    def _parse_post(cls, post, total=-1, offset=-1):
        _post = cls.build_post()
        _post['id'] = post['id']
        _post['total'] = total
        _post['offset'] = offset
        _post['blog_name'] = post['blog_name']
        _post['blog_root'] = cls._parse_blog_root(post)
        _post['type'] = post['type']
        _post['remote_gmt'] = post['date']
        _post['local_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(post['timestamp']))
        _post['tags'] = (','.join(post['tags']) if post['tags'] else '').replace('\'', '\'\'')
        _post['note_count'] = post['note_count']
        return _post

    @classmethod
    def _parse_post_answer(cls, _post, post):
        _post['media_url'] = cls.__possible(post, list(['asking_url', 'post_url', 'source_url']))
        _post['extension'] = '.' + post['format']
        _post['media_key'] = os.path.basename(_post['media_url'])
        return _post

    @classmethod
    def _parse_post_audio(cls, _post, post):
        _url = cls.__possible(post, list(['audio_url', 'audio_source_url', 'post_url', 'source_url']))
        _url = _url if _url.startswith('https:') else _url.replace('http:', 'https:')
        _post['type'] = '%s:%s' % (post['type'], post['audio_type'])
        _ret = urlparse(_url)
        _name = os.path.split(_url)[1]
        if _name and _name.startswith('tumblr_') and _ret.scheme == 'https' and _ret.netloc.endswith('.tumblr.com'):
            _key = str(_name)[7:7 + 17]
        elif _ret.netloc.endswith('api.soundcloud.com'):
            _key = _ret.path.split('/')[2]
        elif _ret.netloc.endswith('open.spotify.com'):
            _key = _name
        else:
            _key = ''

        _post['media_url'] = _url
        _post['extension'] = '.mp3'
        _post['media_key'] = _key

        return _post

    @classmethod
    def _parse_post_chat(cls, _post, post):
        _post['media_url'] = cls.__possible(post, list(['url', 'post_url', 'source_url']))
        _post['extension'] = '.' + post['format']
        _post['media_key'] = os.path.basename(_post['media_url'])
        return _post

    @classmethod
    def _parse_post_link(cls, _post, post):

        _url_pattern = re.compile(
            r'^(?:http|ftp)s?://'
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'
            r'localhost|'
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
            r'(?::\d+)?'
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)

        for _urlkey in ('url', 'title', 'summary', 'post_url', 'source_url'):
            if not post.get(_urlkey) or not post[_urlkey]:
                continue
            _urlkey = post[_urlkey]
            _match = _url_pattern.findall(_urlkey)
            if not _match:
                continue
            __urls = '\n'.join(_match)
            __host = urlparse(_match[0]).hostname.split('.')[-2]
            try:
                __key = os.path.split(_match[0])[1].split('=')[1]
            except (IndexError, TypeError):
                __key = os.path.split(_match[0])[1]
            break

        try:
            _post['type'] = '%s:%s' % (post['type'], __host)
            _post['media_url'] = __urls
            _post['extension'] = '.' + post['format']
            _post['media_key'] = __key
        except (AttributeError, IndexError, UnboundLocalError) as err:
            RALog.e('[link]%s, %s' % (post['post_url'], str(err)))

        return _post

    @classmethod
    def _parse_post_quote(cls, _post, post):
        _post['media_url'] = cls.__possible(post, list(['url', 'post_url', 'source_url']))
        _post['extension'] = '.' + post['format']
        _post['media_key'] = os.path.basename(_post['media_url'])
        return _post

    @classmethod
    def _parse_post_text(cls, _post, post):
        _post['media_url'] = cls.__possible(post, list(['url', 'post_url', 'source_url']))
        _post['extension'] = '.' + post['format']
        _post['media_key'] = os.path.basename(_post['media_url'])
        return _post

    @classmethod
    def __parse_photo_url(cls, _url):
        _url = urllib.parse.unquote(_url)
        _purl = _url if _url.startswith('https:') else _url.replace('http:', 'https:')
        _pret = urlparse(_purl)
        if _url.find('.media.tumblr.com') != -1 and not _pret.netloc.endswith('.media.tumblr.com'):
            _purl = 'https://' + '/'.join(_url.split('/')[-3:])
            _pret = urlparse(_purl)
        _pname = os.path.split(_purl)[1]
        _pext = os.path.splitext(_pname)[1]
        if _pret.scheme == 'https' and _pret.netloc.endswith('.media.tumblr.com'):
            if _pname.startswith('tumblr_inline_'):
                _pkey = str(_pname)[14:14 + 18]
            elif _pname.startswith('tumblr_'):
                _pkey = str(_pname)[7:7 + 18]
            else:
                _pkey = str(_pname).split('_')[0]
            return {'url': _purl, 'key': _pkey, 'ext': _pext, 'name': _pname}
        elif _pret.netloc.endswith('assets.tumblr.com'):
            if _pname.startswith('community_guidelines_'):
                return {'url': '', 'key': '', 'ext': '', 'name': ''}
            else:
                RALog.e('[photo]parser %s' % _url)
        return {'url': '', 'key': '', 'ext': '', 'name': ''}

    @classmethod
    def _parse_post_photo(cls, _post, post):

        _purls = list(map(
            lambda photo: photo['original_size']['url'], post['photos']))
        _purls = list(map(
            lambda _purl: _purl if _purl.find('assets.tumblr.com') == -1 else '', _purls))
        _purls = list(sorted(set(_purls)))
        if '' in _purls:
            _purls.remove('')

        _photos = list(map(cls.__parse_photo_url, _purls))

        _pexts = list(sorted(set(list(map(lambda __photo: __photo['ext'], _photos)))))
        _pkeys = list(sorted(set(list(map(lambda __photo: __photo['key'], _photos)))))

        _post['media_url'] = '\n'.join(_purls)
        _post['extension'] = '\n'.join(_pexts)
        _post['media_key'] = ','.join(_pkeys)

        return _post

    @staticmethod
    def __parse_url(_url):
        _fixurl = urllib.parse.unquote(_url)
        _pattern = r'^(https|http)?://t.umblr.com/redirect\?z=((https|http)?:\/\/)[^\s]+&t='
        _compile = re.compile(_pattern)
        _match = _compile.match(_fixurl)
        if _match:
            _fixurl = _match.group(0).split('=')[1][0:-2]
        _fixurl = _fixurl if _fixurl.startswith('https:') else _fixurl.replace('http:', 'https:')
        return _fixurl

    @staticmethod
    def __parse_post_video(_url):
        _redict = dict({
            r'^https://(vtt|vt|v)?.(media.)?tumblr.com/tumblr_[0-9a-zA-Z]{17}(_r[0-9]{1,2})?(_[0-9]{3,4})?.(mp4|mov)?$': lambda
                _ukey: _ukey[7:7 + 17],
            r'^https://[0-9a-zA-Z\-]+.tumblr.com/post/[0-9]{9,12}$': lambda _ukey: _ukey,
            r'^https://[0-9a-zA-Z\-]+.tumblr.com/post/[0-9]{9,12}': lambda _ukey: _ukey,
            r'^https://(www.)?[0-9a-zA-Z\-]+.tumblr.com($|/tagged/)?': lambda _ukey: '',
            r'^https://(www.)?vine.co/v/[0-9a-zA-Z]{11}$': lambda _ukey: _ukey,
            r'^https://(www.)?instagram.com/(p/)?[0-9a-zA-Z\-\_]{10,39}(/\?taken-)?$': lambda _ukey: _ukey,
            r'^https://(www.)?youtube.com/watch\?v=[0-9a-zA-Z\-]{11}$': lambda _ukey: _ukey[8:],
            r'^https://(www.)?vimeo.com/(p/)?[0-9a-zA-Z]{7,10}$': lambda _ukey: _ukey,
            r'^https://(www.)?vimeo.com/(p/)?[0-9a-zA-Z]{7,10}\?': lambda _ukey: _ukey,
            r'^https://(www.)?funnyordie.com/videos/[0-9a-zA-Z]{10}$': lambda _ukey: _ukey,
            r'^https://(www.)?getkanvas.com/e/[0-9a-zA-Z]{24}$': lambda _ukey: _ukey,
            r'^https://(www.)?gettyimages.com/detail/[0-9]{9}$': lambda _ukey: _ukey,
            r'^https://(www.)?flickr.com/photos/[0-9]{7,10}@N[0-9]{1,2}/[0-9]{10,11}$': lambda _ukey: _ukey,
            r'^https://(www.)?flickr.com/photos/[0-9a-zA-Z\-\_]+/[0-9]{9,11}': lambda _ukey: _ukey,
            r'^https://(www.)?xtube.com/video-watch/[0-9a-zA-Z\-]+': lambda _ukey: _ukey,
            r'^https://(www.)?xtube.com/[0-9a-zA-Z\-]+.php$': lambda _ukey: '',
            r'^https://(www.)?collegehumor.com/video/[0-9]{7}$': lambda _ukey: _ukey,
            r'^https://(www.)?cockdaze.com/post/[0-9]{12}': lambda _ukey: _ukey,
            r'^https://(www.)?redtube.com/[0-9]{6}': lambda _ukey: _ukey,
            r'^https://(www.)?dailymotion.com/video/[0-9a-zA-Z]{6,7}$': lambda _ukey: _ukey,
            r'^https://(www.)?xvideos.com/video[0-9]{7}': lambda _ukey: _ukey,
            r'^https://(www.)?kickstarter.com/projects/[0-9]{10}': lambda _ukey: _ukey,
            r'^https://(www.)?myvidster.com/video/[0-9]{7}': lambda _ukey: _ukey,
            r'^https://(www.)?myvidster.com/user/quickadd.php': lambda _ukey: '',
            r'^https://(www.)?raunchyfuckers.com/player/i[0-9]{4}': lambda _ukey: _ukey,
            r'^https://(www.)?reddit.com/r/[0-9a-zA-Z\-\_]+/comments/[0-9a-zA-Z\-\_]{6}/[0-9a-zA-Z\-\_]+': lambda
                _ukey: _ukey,
            r'^https://(www.)?jocknotized.com/post/[0-9]{11}': lambda _ukey: _ukey,
            r'^https://(www.)?specsaddicted.com/blog/[0-9]{4}/[0-9]{2}/[0-9]{2}/[\u4E00-\u9FA50-9a-zA-Z\-]+': lambda
                _ukey: _ukey,
            r'^https://(www.)?youtube.com(/watch)?$': lambda _ukey: '',
            r'^https://(www.)?pornhub.com/view_video.php': lambda _ukey: '',
            r'^https://(www.)?twitter.com/cursedtactics/status/[0-9]{18}': lambda _ukey: _ukey,
            r'^https://(www.)?blog.daddyissues.net/post/[0-9]{12}': lambda _ukey: _ukey,
            r'^https://(www.)?vine.co/u/[0-9]{19}': lambda _ukey: _ukey,
            r'^https://(www.)?vk.com/video[0-9\_]{19}': lambda _ukey: _ukey,
            r'^https://(www.)?(thecommonchick|weloveshortvideos|instagram)?.com$': lambda _ukey: '',
            r'^https://(www.)?(|vimeo|david\-sf|cumdumpguys|BoyCrazed|blog.madsweat)?.com$': lambda _ukey: ''
        })
        for _reurl, _funkey in _redict.items():
            _url = _url.rstrip('/')
            _compile = re.compile(_reurl, re.IGNORECASE)
            _match = _compile.match(_url)
            if not _match:
                continue
            __name = os.path.split(_match.group(0))[1]
            __key = _funkey(__name)
            __ext = os.path.splitext(__name)[1] if __key else ''
            return __key, __ext
        return '', ''

    @classmethod
    def _parse_post_video(cls, _post, post):
        _post['type'] = '%s:%s' % (post['type'], post['video_type'])

        _vurl = cls.__possible(
            post, list(['video_url', 'permalink_url', 'source_url', 'post_url']))
        _vurl = cls.__parse_url(_vurl)

        _vkey, _vext = cls.__parse_post_video(_vurl)

        _post['media_url'] = _vurl
        _post['extension'] = _vext
        _post['media_key'] = _vkey

        return _post

    @staticmethod
    def __possible(struct, keys):
        for key in keys:
            if key in struct and struct[key]:
                return struct[key]
        return ''

    @staticmethod
    def __possible_extends(struct, keys):
        _dict = dict()
        for key in keys:
            if key in struct and struct[key]:
                _dict[key] = struct[key]
        return _dict

    @staticmethod
    def cleanurltext(_url_line):
        _url = re.sub('[\r\n\t ]', '', _url_line)
        _url = urllib.parse.unquote(_url)
        _url = _url.replace(' ', '').rstrip('/').replace('#_=_', '')
        for _eflag in ('url=', 'p=', 'ref='):
            if _url.find(_eflag) != -1:
                _url = _url.split(_eflag)[1]
                break
        _url = _url.split('?')[0].rstrip('/')
        return _url

    @staticmethod
    def findtumblrblogitem(_url):
        _url = TumblrParser.cleanurltext(_url)
        _relist = list([
            r'^(http(s)?://)?(?P<blogname>[0-9a-zA-Z\-\/_~%!$&\'()*+]+)$',
            r'^(http(s)?://)?www.tumblr.com(/dashboard/blog/|/login_required)?'
            r'(/)?(?P<blogname>[0-9a-zA-Z\-_~%!$&\'()*+]+)?(/post)?'
            r'(/)?(?P<postid>[0-9]{9,12})?',
            r'^(http(s)?://)?(?P<blogname>[0-9a-zA-Z\-\/_~%!$&\'()*+]+).tumblr.com(/post)?(/)?(?P<postid>[0-9]{9,12})?',
        ])
        _defv = dict({'blogname': '', 'postid': '', 'url': _url})
        for _reurl in _relist:
            _compile = re.compile(_reurl, re.IGNORECASE)
            _ret = _compile.match(_url)
            if not _ret:
                continue
            _ret = _ret.groupdict()
            _defv['blogname'] = _ret['blogname']
            _defv['postid'] = _ret['postid'] if _ret.get('postid') else ''
            _defv['url'] = ('https://%s.tumblr.com/%s' % (_defv['blogname'], _defv['postid'])).rstrip('/')
            break
        if not _defv['blogname']:
            RALog.e('%s' % _defv['url'])
            return None
        return _defv

    @staticmethod
    def findtumblrblogitems(_urls, _postid=False):
        if _postid:
            _blogitems = [_blogitem
                          for _blogitem in list(map(TumblrParser.findtumblrblogitem, _urls)) if _blogitem]
            _blogitems.sort(key=lambda k: (k.get('blogname', ''), k.get('postid', '')))
        else:
            _blogitems = [_blogitem['blogname']
                          for _blogitem in list(map(TumblrParser.findtumblrblogitem, _urls)) if _blogitem]
            _blogitems = list(sorted(set(_blogitems)))
        return _blogitems

    @staticmethod
    def blognamestoblogitems(_blognames):
        if not _blognames:
            return list()
        _blogitems = list([
            {'blogname': _blogname, 'postid': '', 'url': 'https://%s.tumblr.com' % _blogname}
            for _blogname in _blognames])
        _blogitems.sort(key=lambda k: (k.get('blogname', ''), k.get('postid', '')))
        return _blogitems


if __name__ == "__main__":

    _blogstring = CFile.read_string('K:\\tumblrtasker\\tumblr_blog_namels.txt')
    _blogstring = _blogstring.lower().strip(' ').strip('\n')
    _blogstring = re.sub('[\r\t]', '', _blogstring)
    gblognames = list(sorted(set(_blogstring.split('\n'))))

    _ftxt = CFile.read_string('K:\\over.txt')
    _urls = list(sorted(set(_ftxt.split('\n'))))
    _blognames = list(sorted(set(list(map(
        lambda _url: _url.split('.')[0] if _url.endswith('.tumblr.com') else '', _urls)))))
    for _exclude in list(['', '78', 'vt', 'vvt', 'www']):
        if _exclude in _blognames:
            _blognames.remove(_exclude)

    _need_blognames = list(sorted(set(_blognames).difference(set(gblognames))))
    print('\n'.join(_need_blognames))
    for _blogname in _need_blognames:
        print('https://%s.tumblr.com/archive' % _blogname)

    pass
