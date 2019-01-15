#!/usr/bin/env python3

from __future__ import print_function
from ftplib import FTP
import ftplib
import argparse
import sys
import json
import re
import os
import distutils.dir_util
import logging
import requests
from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,
                         BucketAlreadyExists)
from io import BytesIO
from urllib.parse import urlparse
debug = True


def download_ftp_file(source, target, ftp):
    logging.debug('downloading ftp file: ' + source + ' target: ' + target)
    basedir = os.path.dirname(target)
    distutils.dir_util.mkpath(basedir)

    try:
        ftp.retrbinary("RETR " + source, open(target, 'w').write)
    except:
        logging.error('Unable to retrieve file')
        return 1
    return 0


def create_ftp_dir(target, ftp):
    # check if directory exists, if yes just return
    try:
        ftp.cwd(target)
        return
    except ftplib.error_perm:
        pass

    parent = os.path.dirname(target)
    basename = os.path.basename(target)
    logging.debug('parent: ' + parent + ', basename: ' + basename)

    if parent == target:  # we have recursed to root, nothing left to do
        raise RuntimeError('Unable to create parent dir')
    try:
        ftp.cwd(parent)
    except:
        logging.error('cannot stat: ' + parent + ', trying to create parent')
        create_ftp_dir(parent, ftp)
        ftp.cwd(parent)

    logging.debug('Current wd is: ' + ftp.pwd())

    ftp.mkd(basename)


def process_upload_dir(source, target, ftp):
    logging.debug(
        'processing upload dir src: ' +
        source +
        ' target: ' +
        target)
    #logging.debug('dir basename: '+basename)
    wd = ftp.pwd()
    # does the parent dir exist?
    try:
        ftp.cwd('/' + target)
    except:
        ftp.cwd(wd)
        logging.error('Cannot stat parent dir: /' + target + ', creating...')
        create_ftp_dir(target, ftp)
        ftp.cwd('/' + target)

    basename = os.path.basename(source)
    for f in os.listdir(source):
        path = source + '/' + f
        if os.path.isdir(path):
            process_upload_dir(path, target + '/' + basename + '/', ftp)
        elif os.path.isfile(path):
            logging.debug(
                'Trying to upload file: ' +
                path +
                ' to dest: ' +
                target +
                '/' +
                f)
            try:
                ftp.storbinary("STOR " + target + '/' + f, open(path, 'r'))
            except:
                logging.error('Error trying to upload file')
    return 0


def process_ftp_dir(source, target, ftp):
    logging.debug('processing ftp dir: ' + source + ' target: ' + target)
    pwd = ftp.pwd()
    ftp.cwd('/' + source)

    ls = []
    ftp.retrlines('LIST', ls.append)

    # This is horrible and I'm sorry but it works flawlessly. Credit to Chris Haas for writing this.
    # See https://stackoverflow.com/questions/966578/parse-response-from-ftp-list-command-syntax-variations
    # for attribution
    p = re.compile(
        '^(?P<dir>[\-ld])(?P<permission>([\-r][\-w][\-xs]){3})\s+(?P<filecode>\d+)\s+(?P<owner>\w+)\s+(?P<group>\w+)\s+(?P<size>\d+)\s+(?P<timestamp>((\w{3})\s+(\d{2})\s+(\d{1,2}):(\d{2}))|((\w{3})\s+(\d{1,2})\s+(\d{4})))\s+(?P<name>.+)$')
    for l in ls:
        dirbit = p.match(l).group('dir')
        name = p.match(l).group('name')

        if dirbit == 'd':
            process_ftp_dir(source + '/' + name, target + '/' + name, ftp)
        else:
            download_ftp_file(name, target + '/' + name, ftp)

    ftp.cwd(pwd)


def process_ftp_file(ftype, afile):
    p = re.compile('[a-z]+://([-a-z.]+)/(.*)')
    ftp_baseurl = p.match(afile['url']).group(1)
    ftp_path = p.match(afile['url']).group(2)

    logging.debug('Connecting to FTP: ' + ftp_baseurl)
    ftp = FTP(ftp_baseurl)
    if os.environ.get('TESK_FTP_USERNAME') is not None:
        try:
            user = os.environ['TESK_FTP_USERNAME']
            pw = os.environ['TESK_FTP_PASSWORD']
            ftp.login(user, pw)
        except ftplib.error_perm:
            ftp.login()
    else:
        ftp.login()

    if ftype == 'inputs':
        if afile['type'] == 'FILE':
            return download_ftp_file(ftp_path, afile['path'], ftp)
        elif afile['type'] == 'DIRECTORY':
            return process_ftp_dir(ftp_path, afile['path'], ftp)
        else:
            print('Unknown file type')
            return 1
    elif ftype == 'outputs':
        if afile['type'] == 'FILE':
            try:
                # this will do nothing if directory exists so safe to do always
                create_ftp_dir(os.path.dirname(ftp_path), ftp)

                ftp.storbinary("STOR /" + ftp_path, open(afile['path'], 'r'))
            except:
                logging.error(
                    'Unable to store file ' +
                    afile['path'] +
                    ' at FTP location ' +
                    ftp_path)
                raise
                return 1
            return 0
        elif afile['type'] == 'DIRECTORY':
            return process_upload_dir(afile['path'], ftp_path, ftp)
        else:
            logging.error('Unknown file type: ' + afile['type'])
            return 1
    else:
        logging.error('Unknown file action: ' + ftype)
        return 1


def process_http_file(ftype, afile):
    if ftype == 'inputs':
        r = requests.get(afile['url'])

        if r.status_code != 200:
            logging.error('Got status code: ' + str(r.status_code))
            return 1

        fp = open(afile['path'], 'wb')
        fp.write(r.content)
        fp.close
        return 0
    elif ftype == 'outputs':
        fp = open(afile['path'], 'r')
        r = requests.put(afile['url'], data=fp.read())

        if r.status_code != 200 or r.status_code != 201:
            logging.error('Got status code: ' + str(r.status_code))
            return 1

        fp.close
        return 0
    else:
        print('Unknown action')
        return 1


def get_path_folders(whole_path):
    """
    Returns all subfolders in a path, in order


    >>> subfolders_in('this/is/a/path')
    ['this', 'is', 'a', 'path']
    """
    path_fragments = whole_path.lstrip('/').split('/')
    path = path_fragments[0]
    subfolders = [path]
    for fragment in path_fragments[1:]:
        subfolders.append(fragment)
    return subfolders


def subfolders_in(whole_path):
    """
    Returns all subfolders in a path, in order

    >>> subfolders_in('/')
    ['/']

    >>> subfolders_in('/this/is/a/path')
    ['/this', '/this/is', '/this/is/a', '/this/is/a/path']

    >>> subfolders_in('this/is/a/path')
    ['this', 'this/is', 'this/is/a', 'this/is/a/path']
    """
    path_fragments = whole_path.lstrip('/').split('/')
    if whole_path.startswith('/'):
        path_fragments[0] = '/' + path_fragments[0]
    path = path_fragments[0]
    subfolders = [path]
    for fragment in path_fragments[1:]:
        path += '/' + fragment
        subfolders.append(path)
    return subfolders


def get_bucket_object(path):

    subfolders = get_path_folders(path)

    # First dir in path indicates the bucket
    bucket = subfolders.pop(0).lstrip('/')
    # Rest of them indicate the name of the object
    object_name = ""
    for subfolder in subfolders:
        object_name += "/" + subfolder
    object_name = object_name.lstrip("/")
    if object_name.endswith("/"):
        object_name = object_name.rstrip("/")

    return bucket, object_name


def upload_s3_dir(bucket, object_name, source, minio_client):

    for listing in os.listdir(source):

        file_path = source + '/' + listing
        object_path = listing if object_name == "" else object_name + '/' + listing

        if os.path.isdir(file_path):
            logging.debug('Uploading %s\t"%s"', "DIRECTORY", file_path)
            upload_s3_dir(bucket, object_path, file_path, minio_client)

        elif os.path.isfile(file_path):
            logging.debug('Uploading %s\t"%s"', "FILE", file_path)
            upload_s3_file(bucket, object_path, file_path, minio_client)

        else:
            logging.error(
                'Directory listing in is neither file nor directory: "%s"',
                file_path
            )
            return 1
    return 0


def download_s3_dir(bucket, object_name, target, minio_client):
    logging.debug('Downloading s3 dir: %s Target: %s', bucket + "/" + object_name, target)

    subfolders = subfolders_in(object_name)

    offset = len(subfolders[-2]) + 1 if len(subfolders) > 1 else 0

    if not target.endswith("/"):
        target += "/"

    # List the contents of the bucket
    objects = minio_client.list_objects(bucket, object_name, recursive=True)
    for obj in objects:
        print(obj.object_name)

        basedir = os.path.dirname(obj.object_name[offset:])
        file_path = target + basedir
        print(file_path)
        distutils.dir_util.mkpath(file_path)

        file_path += "/" + get_path_folders(obj.object_name).pop(-1)
        print(file_path)

        try:
            minio_client.fget_object(obj.bucket_name, obj.object_name, file_path)

        except ResponseError as err:
            logging.error('Got status code: %d', err.code)
            logging.error(err.message)

            return 1

    return 0


def upload_s3_file(bucket, object_name, source, minio_client):

    try:
        minio_client.fput_object(bucket, object_name, source)
    except ResponseError as err:
        print(err)


def download_s3_file(bucket, object_name, target, minio_client):
    logging.debug('Downloading s3 object: "%s" Target: %s', bucket + "/" + object_name, target)
    basedir = os.path.dirname(target)
    distutils.dir_util.mkpath(basedir)

    try:
        minio_client.fget_object(bucket, object_name, target)
    except ResponseError as err:
        logging.error('Got status code: %d', err.code)
        logging.error(err.message)

        return 1
    return 0


def process_s3_file(ftype, afile):

    parseUrl = urlparse(afile['url'])
    s3_baseurl = parseUrl.netloc
    s3_path = parseUrl.path

    bucket, object_name = get_bucket_object(s3_path)

    # if os.environ.get('TESK_S3_ACCESS_KEY') is not None:

    access_key = "AKIAIOSFODNN7EXAMPLE"                                 #os.environ['TESK_S3_ACCESS_KEY']
    secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"             #os.environ['TESK_S3_SECRET_KEY']
    minio_client = Minio(s3_baseurl, access_key, secret_key, False)

    if ftype == 'inputs':
        if afile['type'] == 'FILE':
            return download_s3_file(bucket, object_name, afile['path'], minio_client)
        elif afile['type'] == 'DIRECTORY':
            return download_s3_dir(bucket, object_name, afile['path'], minio_client)
        else:
            print('Unknown file type')
            return 1
    elif ftype == 'outputs':

        try:
            minio_client.make_bucket(bucket)

        except BucketAlreadyOwnedByYou as err:
            pass
        except BucketAlreadyExists as err:
            pass
        except ResponseError as err:
            raise

        if afile['type'] == 'FILE':
            return upload_s3_file(bucket, object_name, afile['path'], minio_client)
        elif afile['type'] == 'DIRECTORY':
            # tmp = os.path.basename(afile['path'])
            # object_name = tmp if object_name == "" else object_name + "/" + tmp
            return upload_s3_dir(bucket, object_name, afile['path'], minio_client)
        else:
            logging.error('Unknown file type: ' + afile['type'])
            return 1
    else:
        logging.error('Unknown file action: ' + ftype)
        return 1


def filefromcontent(afile):
    content = afile.get('content')
    if content is None:
        logging.error(
            'Incorrect file spec format, no content or url specified')
        return 1

    fh = open(afile['path'], 'w')
    fh.write(str(afile['content']))
    fh.close()
    return 0


def process_file(ftype, afile):
    url = afile.get('url')
    if url is None:
        return filefromcontent(afile)

    protocol = urlparse(url).scheme

    logging.debug('protocol is: ' + protocol)

    if protocol == 'ftp':
        return process_ftp_file(ftype, afile)
    elif protocol == 'http' or protocol == 'https':
        return process_http_file(ftype, afile)
    elif protocol == 's3':
        return process_s3_file(ftype, afile)
    else:
        print('Unknown file protocol')
        return 1


def debug(msg):
    if debug:
        print(msg, file=sys.stderr)


def main(argv):
    logging.basicConfig(
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S',
        level=logging.DEBUG)
    logging.debug('Starting filer...')
    parser = argparse.ArgumentParser(
        description='Filer script for down- and uploading files')
    parser.add_argument(
        'filetype',
        help='filetype to handle, either \'inputs\' or \'outputs\' ')
    parser.add_argument(
        'data',
        help='file description data, see docs for structure')
    args = parser.parse_args()

    data = json.loads(args.data)

    for afile in data[args.filetype]:

        logging.debug('processing file: ' + afile['path'])
        if process_file(args.filetype, afile):
            logging.error('something went wrong')
            return 1
        # TODO a bit more detailed reporting
        else:
            logging.debug('Processed file: ' + afile['path'])

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
