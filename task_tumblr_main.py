# -*- coding: utf-8 -*-
# encoding=utf-8
import sys
import argparse
from task_tumblr_tasker import TumblrTasker


def args_handler(_argv):
    p = argparse.ArgumentParser(
        description='download from tumblr.com')
    ''' sites: '''
    p.add_argument('sites', type=str, nargs='*', help='地址标识.')
    ''' method: '''
    p.add_argument('-p', '--processes', action='store', type=int, default=8,
                   help='指定多进程数,默认为8个,最多为16个 eg: -p 16')
    p.add_argument('-u', '--update', action='store_true', default=False,
                   help='是否更新 eg: -u False')
    p.add_argument('-o', '--out', action='store', type=str, default='',
                   help='指定uri的out目录 eg: \'-u/--uri -o \'xunlei\'')
    ''' range: '''
    p.add_argument('-n', '--number', action='store', type=int, default=-1,
                   help='本地最大存储数量')
    p.add_argument('-s', '--start', action='store', type=str, default='',
                   help='指定起始时间 eg: -s \'2018-01-01 00:00:00\'')
    p.add_argument('-e', '--end', action='store', type=str, default='',
                   help='指定截止时间 eg: -e \'2018-01-01 00:00:00\'')
    p.add_argument('-l', '--limit', action='store', type=int, default=-1,
                   help='指定时间时，最大数量')
    ''' types: answer, audio, chat, link, photo, quote, text, video '''
    p.add_argument('-a', '--answer', action='store_true',
                   help='download answers')
    p.add_argument('-A', '--audio', action='store_true',
                   help='download audios')
    p.add_argument('-C', '--chat', action='store_true',
                   help='download chats')
    p.add_argument('-L', '--link', action='store_true',
                   help='download links')
    p.add_argument('-P', '--photo', action='store_true',
                   help='download photos')
    p.add_argument('-Q', '--quote', action='store_true',
                   help='download quotes')
    p.add_argument('-T', '--text', action='store_true',
                   help='download texts')
    p.add_argument('-V', '--video', action='store_true',
                   help='download videos')
    ''' tags: '''
    p.add_argument('-t', '--tags', action='store',
                   default=None, type=str,
                   help='下载特定tag的posts, eg: -t Hispanic')
    ''' extra: '''
    p.add_argument('--uri', action='store_true', default=False,
                   help='generate uri for download')
    p.add_argument('--rebelong', action='store_true', default=False,
                   help='reset posts belong to blogname')
    p.add_argument('--remove', action='store_true', default=False,
                   help='remove posts to blogname')
    p.add_argument('--discover', action='store_true', default=False,
                   help='discover blog names')

    _args = p.parse_args(_argv[1:])
    _sites = _args.sites

    return _args, _sites


def main(_argv):
    __argv = TumblrTasker.init(_argv)
    print(__argv)

    _args, _sites = args_handler(__argv)

    TumblrTasker.run_task(_args, _sites)

    TumblrTasker.uninit()

    return


if __name__ == "__main__":

    _argv = sys.argv

    if len(_argv) == 1:
        """ update """
        _argv.extend(list(['-u']))

        """ uri and url """
        # _argv.extend(list(['--uri']))

        """ remove """
        # from total_mkeydir.csv
        # _argv.extend(list(['--remove']))

        """ rebelong """
        # total_mkeydir_list.txt => total_mkeydir.csv
        # _argv.extend(list(['--rebelong']))

        """ discover """
        # _argv.extend(list(['--discover']))

    """
    _sites = list([
        'test',
        'http://test.tumblr.com',
        'test',
        'test.tumblr.com'
    ])
    """

    # _argv.extend(list(['test1', 'test2', 'test3', 'test4', 'test5']))
    _argv.extend(list(['-p', '12']))
    _argv.extend(list(['-n', '-1']))
    _argv.extend(list(['-P', '-V']))
    # _argv.extend(list(['-s', '2018-01-01 00:00:00']))
    # _argv.extend(list(['-e', '']))
    # _argv.extend(list(['-l', '1000']))

    main(_argv)
