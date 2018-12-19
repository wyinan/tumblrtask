# -*- coding: utf-8 -*-
# encoding=utf-8
import os
import re
import json
import time
import shutil
from lxml import etree
from task_common import RALog
from task_spider_common import CFile, CNetWork


class CXtube:
    @classmethod
    def init(cls, _xtubedir, _xunleidir, _cacherdir):
        cls.gxtubedir = _xtubedir
        cls.gxunleidir = _xunleidir
        cls.gcacherdir = os.path.join(_cacherdir, '.xtube')
        CFile.mkdirs(cls.gcacherdir)

    @classmethod
    def update_xtube(cls):
        _user = 'jip1006-29480901'
        return cls.update_xtube_user(_user)

    @staticmethod
    def parse_video_watch_codes(_user):
        _url_format = 'https://www.xtube.com/profile/{0}#videos'
        _url_type = _url_format.format(_user)
        _ret, _msg, _response = CNetWork.get_xtube_xml(_url_type)
        if not _ret:
            return False, list()
        _response = _response.replace(" ", "")
        _href_list = re.findall(
            r"<aclass=\"html-attribute-valuehtml-external-link\"target=\"_blank\"href=\"/video-watch/.*?\">/video-watch/.*?<\/a>",
            _response)
        _vcode_list = list(map(lambda _href: str(_href.split('/')[-2]).strip('<'), _href_list))
        return True, _vcode_list

    @classmethod
    def update_xtube_user(cls, _user):

        _xtube_usr_dir = os.path.join(cls.gxtubedir, _user)
        CFile.mkdirs(_xtube_usr_dir)

        _vcodes_txt_path = os.path.join(cls.gcacherdir, 'xtube_' + _user + '.txt')
        # _ret, _vcode_list = cls.parse_video_watch_codes(_user)
        _ret, _vcode_list = True, list()
        if not _ret or not _vcode_list:
            RALog.e('parse video watch codes failed.')

        if _vcode_list:
            _vcode_list = CFile.write_list(_vcodes_txt_path, _vcode_list, _mode='a')
        else:
            _vcode_list = CFile.read_list(_vcodes_txt_path)

        _xtube = dict()
        for _vcode in _vcode_list:
            _xtube[_vcode] = dict()
            _xtube[_vcode]['page_title'] = ''
            _xtube[_vcode]['page_url'] = str('https://www.xtube.com/video-watch/%s' % _vcode)
            _xtube[_vcode]['video_title'] = ''
            _xtube[_vcode]['video_content'] = ''
            _xtube[_vcode]['video_hd_url'] = ''
            _xtube[_vcode]['video_hd_name'] = ''
            _xtube[_vcode]['video'] = {}

        _xtube_json_path = os.path.join(cls.gcacherdir, 'xtube_' + _user + '.json')
        _xtube_local = CFile.read_dict(_xtube_json_path)

        for _vcode, _video in _xtube.items():
            if _vcode not in _xtube_local.keys():
                _xtube_local[_vcode] = _video

        _xtube_local = dict(sorted(_xtube_local.items(), key=lambda d: d[0]))
        _total = len(_xtube_local)
        _index = 0
        for _vcode, _video in _xtube_local.items():
            _index = _index + 1

            if _xtube_local[_vcode]['video_hd_url']:
                continue

            _page_url = _video['page_url']
            _tc_start = time.clock()
            _htmltxt = CNetWork.get_xtube_xml(_page_url)  # CNetWork.get_html_text(_page_url)
            _tc_end = time.clock()

            RALog.i("[%s][Time:%0.3f][%03d/%03d]%s" % (_user, _tc_end - _tc_start, _index, _total, _page_url))

            _html = etree.HTML(_htmltxt)

            _element_list = _html.xpath('//title/text()')
            if _element_list and len(_element_list) == 1:
                _xtube_local[_vcode]['page_title'] = str(_element_list[0]).replace('\n', '').strip()

            _element_list = _html.xpath('//article[@class="cntBox contentInfo"]//h1//text()')
            if _element_list and len(_element_list) == 1:
                _xtube_local[_vcode]['video_title'] = str(_element_list[0]).replace('\n', '').strip()

            _element_list = _html.xpath('//article[@class="cntBox contentInfo"]//p//text()')
            if _element_list and len(_element_list) == 1:
                _xtube_local[_vcode]['video_content'] = str(_element_list[0]).replace('\n', '').strip()

            _element_list = _html.xpath('//div[@class="expandStage"]//script[contains(text(), "playerConf")]//text()')
            if _element_list and len(_element_list) == 1:
                _script_list = str(_element_list[0]).split('\n')
                if _script_list and len(_script_list) > 2:
                    _script = str(_script_list[1]).strip().replace('\\', '')
                    _sources = re.findall(r'"sources":{.*?}', _script)
                    if _sources and len(_sources) == 1:
                        _vsource = re.findall(r'"\d+":".*?"', _sources[0])
                        if _vsource and len(_vsource):
                            for _vpurl in _vsource:
                                _vp = str(_vpurl.split('"')[1])
                                _vurl = str(_vpurl.split('"')[3])
                                _xtube_local[_vcode]['video'][_vp] = _vurl

                            _hd_value = max(list(_xtube_local[_vcode]['video'].keys()))
                            _hd_url = _xtube_local[_vcode]['video'][_hd_value]
                            _hd_name = str(str(_hd_url.split('?')[0]).split('/')[-1])
                            _xtube_local[_vcode]['video_hd_url'] = _hd_url
                            _xtube_local[_vcode]['video_hd_name'] = _hd_name
            continue

        with open(_xtube_json_path, 'w+') as _xtube_json:
            _xtube_json.write(json.dumps(_xtube_local, ensure_ascii=False, indent=4))
            _xtube_json.close()

        for _vcode, _video in _xtube_local.items():
            _hd_name = _video['video_hd_name']
            # _hd_url = _video['video_hd_url']
            _usr_path = os.path.join(_xtube_usr_dir, _hd_name)
            if os.path.exists(_usr_path):
                continue
            _xl_path = os.path.join(cls.gxunleidir, _hd_name)
            if os.path.exists(_xl_path):
                shutil.move(_xl_path, _usr_path)
                continue
            print(_video['video_title'])
            print(_video['page_url'])
            print(_video['video_hd_url'])

        return
