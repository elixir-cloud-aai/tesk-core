#!/usr/bin/env python3

from __future__ import print_function
from ftplib import FTP
import ftplib
import argparse
import sys
import json
import re
import os
import enum
import distutils.dir_util
import logging
import requests
import boto3
import botocore

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse


@enum.unique
class Type(enum.Enum):
    File = 'FILE'
    Directory = 'DIRECTORY'


class Transput:
    def __init__(self, path, url, ftype):
        self.path = path
        self.url = url
        self.ftype = ftype

        parsed_url = urlparse(url)
        self.netloc = parsed_url.netloc
        self.url_path = parsed_url.path

    def upload(self):
        logging.debug('%s uploading %s %s', self.__class__.__name__, self.ftype, self.url)
        if self.ftype == Type.File:
            return self.upload_file()
        if self.ftype == Type.Directory:
            return self.upload_dir()
        return 1

    def download(self):
        logging.debug('%s downloading %s %s', self.__class__.__name__, self.ftype, self.url)
        if self.ftype == Type.File:
            return self.download_file()
        if self.ftype == Type.Directory:
            return self.download_dir()
        return 1

    def delete(self):
        pass

    def download_file(self):
        raise NotImplementedError()

    def download_dir(self):
        raise NotImplementedError()

    def upload_file(self):
        raise NotImplementedError()

    def upload_dir(self):
        raise NotImplementedError()

    # make it compatible with contexts (with keyword)
    def __enter__(self):
        return self

    def __exit__(self, error_type, error_value, traceback):
        self.delete()
        # Swallow all exceptions since the filer mostly works with error codes
        return False


class HTTPTransput(Transput):
    def __init__(self, path, url, ftype):
        Transput.__init__(self, path, url, ftype)

    def download_file(self):
        req = requests.get(self.url)

        if req.status_code < 200 or req.status_code >= 300:
            logging.error('Got status code: %d', req.status_code)
            logging.error(req.text)
            return 1
        logging.debug('OK, got status code: %d', req.status_code)

        with open(self.path, 'wb') as file:
            file.write(req.content)
        return 0

    def upload_file(self):
        with open(self.path, 'r') as file:
            file_contents = file.read()
        req = requests.put(self.url, data=file_contents)

        if req.status_code < 200 or req.status_code >= 300:
            logging.error('Got status code: %d', req.status_code)
            logging.error(req.text)
            return 1
        logging.debug('OK, got status code: %d', req.status_code)

        return 0

    def upload_dir(self):
        to_upload = []
        for listing in os.listdir(self.path):
            file_path = self.path + '/' + listing
            if os.path.isdir(file_path):
                ftype = Type.Directory
            elif os.path.isfile(file_path):
                ftype = Type.File
            else:
                return 1
            to_upload.append(HTTPTransput(file_path, self.url + '/' + listing, ftype))

        # return 1 if any upload failed
        return min(sum([transput.upload() for transput in to_upload]), 1)

    def download_dir(self):
        logging.error(
            'Won\'t crawl http directory, so unable to download url: %s',
            self.url)
        return 1


class FTPTransput(Transput):
    def __init__(self, path, url, ftype, ftp_conn=None):
        Transput.__init__(self, path, url, ftype)

        self.connection_owner = ftp_conn is None
        self.ftp_connection = FTP() if ftp_conn is None else ftp_conn

    # entice users to use contexts when using this class
    def __enter__(self):
        if self.connection_owner:
            self.ftp_connection.connect(self.netloc)
            ftp_login(self.ftp_connection)
        return self

    def upload_dir(self):
        for file in os.listdir(self.path):
            file_path = self.path + '/' + file
            file_url = self.url + '/' + file

            if os.path.isdir(file_path):
                ftype = Type.Directory
            elif os.path.isfile(file_path):
                ftype = Type.File
            else:
                logging.error(
                    'Directory listing in is neither file nor directory: "%s"',
                    file_url
                )
                return 1

            logging.debug('Uploading %s\t"%s"', ftype.value, file_path)

            # We recurse into new transputs, ending with files which are uploaded
            # Downside is nothing happens with empty dirs.
            with FTPTransput(file_path, file_url, ftype) as transfer:
                if transfer.upload():
                    return 1
        return 0

    def upload_file(self):
        error = ftp_make_dirs(self.ftp_connection, os.path.dirname(self.url_path))
        if error:
            logging.error(
                'Unable to create remote directories needed for %s',
                self.url
            )
            return 1

        if not ftp_check_directory(self.ftp_connection, self.url_path):
            return 1

        return ftp_upload_file(self.ftp_connection, self.path, self.url_path)

    def download_dir(self):
        logging.debug('Processing ftp dir: %s target: %s', self.url, self.path)
        self.ftp_connection.cwd(self.url_path)

        # This is horrible and I'm sorry but it works flawlessly.
        # Credit to Chris Haas for writing this
        # See https://stackoverflow.com/questions/966578/parse-response-from-ftp-list-command-syntax-variations
        # for attribution
        ftp_command = re.compile(
            r'^(?P<dir>[\-ld])(?P<permission>([\-r][\-w][\-xs]){3})\s+(?P<filecode>\d+)\s+(?P<owner>\w+)\s+(?P<group>\w+)\s+(?P<size>\d+)\s+(?P<timestamp>((\w{3})\s+(\d{2})\s+(\d{1,2}):(\d{2}))|((\w{3})\s+(\d{1,2})\s+(\d{4})))\s+(?P<name>.+)$')

        lines = []
        self.ftp_connection.retrlines('LIST', lines.append)

        for line in lines:
            matches = ftp_command.match(line)
            dirbit = matches.group('dir')
            name = matches.group('name')

            file_path = self.path + '/' + name
            file_url = self.url + '/' + name

            if dirbit == 'd':
                ftype = Type.Directory
            else:
                ftype = Type.File

            # We recurse into new transputs, ending with files which are downloaded
            # Downside is nothing happens with empty dirs.
            with FTPTransput(file_path, file_url, ftype, self.ftp_connection) as transfer:
                if transfer.download():
                    return 1
        return 0

    def download_file(self):
        logging.debug('Downloading ftp file: "%s" Target: %s', self.url, self.path)
        basedir = os.path.dirname(self.path)
        distutils.dir_util.mkpath(basedir)

        return ftp_download_file(self.ftp_connection, self.url_path, self.path)

    def delete(self):
        if self.connection_owner:
            self.ftp_connection.close()


def ftp_login(ftp_connection):
    if 'TESK_FTP_USERNAME' in os.environ and 'TESK_FTP_PASSWORD' in os.environ:
        user = os.environ['TESK_FTP_USERNAME']
        password = os.environ['TESK_FTP_PASSWORD']
        try:
            ftp_connection.login(user, password)
        except ftplib.error_perm:
            ftp_connection.login()
    else:
        ftp_connection.login()


def ftp_check_directory(ftp_connection, path):
    """
    Following convention with the rest of the code,
    return 0 if it is a directory, 1 if it is not or failed to do the check
    """
    response = ftp_connection.pwd()
    if response == '':
        return 1
    original_directory = response

    # We are NOT scp, so we won't create a file when filename is not
    # specified (mirrors input behaviour)
    try:
        ftp_connection.cwd(path)
        logging.error(
            'Path "%s" at "%s" already exists and is a folder. \
            Please specify a target filename and retry',
            path, ftp_connection.host)
        is_directory = True
    except ftplib.error_perm:
        is_directory = False
    except (ftplib.error_reply, ftplib.error_temp):
        logging.exception('Could not check if path "%s" in "%s" is directory',
                          path, ftp_connection.host)
        return 1
    try:
        ftp_connection.cwd(original_directory)
    except (ftplib.error_reply, ftplib.error_perm, ftplib.error_temp):
        logging.exception('Error when checking if "%s" in "%s" was a directory',
                          path, ftp_connection.host)
        return 1

    return 0 if is_directory else 1


def ftp_upload_file(ftp_connection, local_source_path, remote_destination_path):
    try:
        with open(local_source_path, 'r+b') as file:
            ftp_connection.storbinary("STOR /" + remote_destination_path, file)
    except (ftplib.error_reply, ftplib.error_perm):
        logging.exception(
            'Unable to upload file "%s" to "%s" as "%s"',
            local_source_path,
            ftp_connection.host,
            remote_destination_path)
        return 1
    except ftplib.error_temp:
        logging.exception(
            'Unable to upload file "%s" to "%s" as "%s"',
            local_source_path,
            ftp_connection.host,
            remote_destination_path)
        return 1
    return 0


def ftp_download_file(ftp_connection, remote_source_path, local_destination_path):
    try:
        with open(local_destination_path, 'w+b') as file:
            ftp_connection.retrbinary("RETR " + remote_source_path, file.write)
    except (ftplib.error_reply, ftplib.error_perm, ftplib.error_temp):
        logging.exception(
            'Unable to download file "%s" from "%s" as "%s"',
            remote_source_path,
            ftp_connection.host,
            local_destination_path
        )
        return 1
    return 0


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


def ftp_make_dirs(ftp_connection, path):
    response = ftp_connection.pwd()
    if response == '':
        return 1
    original_directory = response

    # if directory exists do not do anything else
    try:
        ftp_connection.cwd(path)
        return 0
    except (ftplib.error_perm, ftplib.error_temp):
        pass
    except ftplib.error_reply:
        logging.exception('Unable to create directory "%s" at "%s"',
                          path, ftp_connection.host)
        return 1

    for subfolder in subfolders_in(path):
        try:
            ftp_connection.cwd(subfolder)
        except (ftplib.error_perm, ftplib.error_temp):
            try:
                ftp_connection.mkd(subfolder)
            except (ftplib.error_reply, ftplib.error_perm, ftplib.error_temp):
                logging.exception('Unable to create directory "%s" at "%s"',
                                  subfolder, ftp_connection.host)
                return 1
        except ftplib.error_reply:
            logging.exception('Unable to create directory "%s" at "%s"',
                              path, ftp_connection.host)
            return 1

    try:
        ftp_connection.cwd(original_directory)
    except (ftplib.error_reply, ftplib.error_perm, ftplib.error_temp):
        logging.exception('Unable to create directory "%s" at "%s"',
                          path, ftp_connection.host)
        return 1
    return 0


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


class S3Transput(Transput):

    def __init__(self, path, url, ftype):
        Transput.__init__(self, path, url, ftype)
        self.bucket, self.object_name = self.get_bucket_object()

    # entice users to use contexts when using this class
    def __enter__(self):
        access_key, secret_key = self.get_access_keys()
        self.client = boto3.resource('s3',
                                     endpoint_url="http://" + self.netloc,
                                     aws_access_key_id=access_key,
                                     aws_secret_access_key=secret_key)

        return self

    def get_access_keys(self):

        if 'TESK_S3_ACCESS_KEY' in os.environ and 'TESK_S3_SECRET_KEY' in os.environ:
            return os.environ['TESK_S3_ACCESS_KEY'], os.environ['TESK_S3_SECRET_KEY']

        return None, None

    def upload_file(self):

        try:
            bucket = self.client.Bucket(self.bucket)
            bucket.upload_file(Filename=self.path, Key=self.object_name)
        except botocore.exceptions.ClientError as err:
            print(err)

        return 0

    def upload_dir(self):

        for listing in os.listdir(self.path):

            file_path = self.path + '/' + listing
            object_path = self.url + '/' + listing
            #
            if os.path.isdir(file_path):
                ftype = Type.Directory
            elif os.path.isfile(file_path):
                ftype = Type.File
            else:
                logging.error(
                    'Directory listing in is neither file nor directory: "%s"',
                    file_path
                )
                return 1

            logging.debug('Uploading %s\t"%s"', ftype.value, file_path)

            with S3Transput(file_path, object_path, ftype) as transfer:
                if transfer.upload():
                    return 1
        return 0

    def download_file(self):

        logging.debug('Downloading s3 object: "%s" Target: %s', self.bucket + "/" + self.object_name, self.path)
        basedir = os.path.dirname(self.path)
        distutils.dir_util.mkpath(basedir)

        try:
            bucket = self.client.Bucket(self.bucket)
            bucket.download_file(Filename=self.path, Key=self.object_name)
        except botocore.exceptions.ClientError as err:
            logging.error('Got status code: %d', err.response['Error']['Code'])
            logging.error(err.response['Error']['Message'])

            return 1
        return 0

    def download_dir(self):

        logging.debug('Downloading s3 dir: %s Target: %s', self.bucket + "/" + self.object_name, self.path)

        subfolders = subfolders_in(self.object_name)

        offset = len(subfolders[-2])+1 if len(subfolders) > 1 else 0

        if not self.path.endswith("/"):
            self.path += "/"

        # List the contents of the bucket
        response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=self.object_name)

        for obj in response['Contents']:

            basedir = os.path.dirname(obj['Key'][offset:])
            file_path = self.path + basedir
            distutils.dir_util.mkpath(file_path)

            file_path += "/" + get_path_folders(obj['Key']).pop(-1)

            try:
                bucket = self.client.Bucket(self.bucket)
                bucket.download_file(Filename=file_path, Key=obj['Key'])

            except botocore.exceptions.ClientError as err:
                logging.error('Got status code: %d', err.response['Error']['Code'])
                logging.error(err.response['Error']['Message'])

                return 1
        return 0

    def get_bucket_object(self):

        subfolders = get_path_folders(self.url_path)

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


def file_from_content(filedata):
    with open(filedata['path'], 'w') as file:
        file.write(str(filedata['content']))
    return 0


def process_file(ttype, filedata):
    if 'content' in filedata:
        return file_from_content(filedata)

    scheme = urlparse(filedata['url']).scheme
    if scheme == '':
        logging.error('Could not determine protocol for url: "%s"', filedata['url'])
        return 1

    if scheme == 'ftp':
        trans = FTPTransput
    elif scheme in ['http', 'https']:
        trans = HTTPTransput
    elif scheme in ['s3']:
        trans = S3Transput
    else:
        logging.error('Unknown protocol "%s" in url "%s"', scheme, filedata['url'])
        return 1

    with trans(filedata['path'], filedata['url'], Type(filedata['type'])) as transfer:
        if ttype == 'inputs':
            return transfer.download()
        if ttype == 'outputs':
            return transfer.upload()

    logging.info('There was no action to do with %s', filedata['path'])
    return 0


def main():
    parser = argparse.ArgumentParser(
        description='Filer script for down- and uploading files')
    parser.add_argument(
        'transputtype',
        help='transput to handle, either \'inputs\' or \'outputs\' ')
    parser.add_argument(
        'data',
        help='file description data, see docs for structure')
    parser.add_argument(
        '--debug',
        '-d',
        help='debug logging',
        action='store_true')
    args = parser.parse_args()

    if args.debug:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.ERROR

    logging.basicConfig(
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S',
        level=loglevel)

    logging.info('Starting %s filer...', args.transputtype)

    data = json.loads(args.data)

    for afile in data[args.transputtype]:
        logging.debug('Processing file: %s', afile['path'])
        if process_file(args.transputtype, afile) != 0:
            logging.error('Unable to process file, aborting')
            return 1
        logging.debug('Processed file: %s', afile['path'])

    return 0


if __name__ == "__main__":
    sys.exit(main())
