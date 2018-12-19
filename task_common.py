# -*- coding: utf-8 -*-
"""
Created on Mon May 29 17:34:17 2017
任务的公共模块
@author: WYiNan
"""
import io
import os
import sys
import gzip
import urllib
import logging
import pymssql
import requests
import datetime
import http.client

from sqlalchemy import create_engine
from datetime import datetime

tmpdir = '%s\\%s\\' % (os.environ['TMP'], 'RedAlert')
os.makedirs(tmpdir, exist_ok=True)


class RALog:
    glogdir = 'G:\\tumblrtasker\\.tumblrcacher\\.log\\'
    os.makedirs(glogdir, 0o777, True)
    gfilename = os.path.join(glogdir,
                             os.path.basename(sys.argv[0].split('.')[0])
                             + '_'
                             + datetime.now().strftime('%Y%m%d%H%M%S')
                             + '.log')

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=gfilename,
                        filemode='w')

    _console = logging.StreamHandler()
    _console.setLevel(logging.INFO)
    _formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    _console.setFormatter(_formatter)
    logger = logging.getLogger('')
    logger.addHandler(_console)

    def __init__(self):
        return

    @staticmethod
    def i(msg):
        RALog.logger.info(msg)

    @staticmethod
    def w(msg):
        RALog.logger.warning(msg)

    @staticmethod
    def e(msg):
        RALog.logger.error(msg)

    @staticmethod
    def d(msg):
        RALog.logger.debug(msg)


class RedMatrixDB:
    _connstr = str.format('mssql+pymssql://%s:%s@%s:%s/%s' % (
        'red', 'red', 'DESKTOP-LHB9HMD', '1433', 'RedMatrix'
    ))
    raengine = create_engine(_connstr, encoding='latin1',
                             connect_args={'charset': 'utf8'},
                             echo=False)

    def __init__(self):
        return


class RedAlertDB:
    _connstr = str.format('mssql+pymssql://%s:%s@%s:%s/%s' % (
        'red', 'red', 'DESKTOP-LHB9HMD', '1433', 'RedAlert'
    ))
    raengine = create_engine(_connstr, encoding='latin1',
                             connect_args={'charset': 'utf8'},
                             echo=False)

    def __init__(self):
        return


def gethtmltext(_url):
    r = requests.get(_url, timeout=60)
    if r.status_code != 200:
        RALog.e('%d %s' % (r.status_code, _url))
        return ''
    else:
        return r.text


def stockcodetosymbol(code):
    if len(code) != 6:
        return ''
    else:
        return 'sh%s' % code if code[:1] in ['5', '6', '9'] else 'sz%s' % code


class DataYesClient:
    HTTP_OK = 200
    HTTP_AUTHORIZATION_ERROR = 401

    domain = 'api.wmcloud.com'
    port = 443
    token = 'c2a17534f3f01858d61352dc49370ee0b5f6239378071e77cf5526934cf481c5'

    # 设置因网络连接，重连的次数
    reconnectTimes = 2
    httpClient = None

    def __init__(self):
        self.httpClient = http.client.HTTPSConnection(self.domain, self.port, timeout=60)

    def __del__(self):
        if self.httpClient is not None:
            self.httpClient.close()

    @staticmethod
    def encodepath(path):
        # 转换参数的编码
        start = 0
        n = len(path)
        re = ''
        i = path.find('=', start)
        while i != -1:
            re += path[start:i + 1]
            start = i + 1
            i = path.find('&', start)
            if i >= 0:
                for j in range(start, i):
                    if path[j] > '~':
                        re += urllib.quote(path[j])
                    else:
                        re += path[j]
                re += '&'
                start = i + 1
            else:
                for j in range(start, n):
                    if path[j] > '~':
                        re += urllib.quote(path[j])
                    else:
                        re += path[j]
                start = n
            i = path.find('=', start)
        return re

    def init(self, token):
        self.token = token

    def getdata(self, path):
        result = None
        path = '/data/v1' + path
        # print(path)
        path = self.encodepath(path)
        for i in range(self.reconnectTimes):
            try:
                # set http header here
                self.httpClient.request('GET', path,
                                        headers={"Authorization": "Bearer " + self.token,
                                                 "Accept-Encoding": "gzip, deflate"}
                                        )
                # make request
                response = self.httpClient.getresponse()
                result = response.read()
                compressedstream = io.BytesIO(result)
                gziper = gzip.GzipFile(fileobj=compressedstream)
                try:
                    result = gziper.read()
                except OSError:
                    pass
                if path.find('.csv?') == -1:
                    result = result.decode('utf-8').encode('GB18030')
                return response.status, result
            except:
                if i == self.reconnectTimes - 1:
                    raise
                if self.httpClient is not None:
                    self.httpClient.close()
                self.httpClient = http.client.HTTPSConnection(
                    self.domain, self.port, timeout=60)

        return -1, result
