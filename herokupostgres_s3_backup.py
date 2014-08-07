#! /usr/bin/env python
"""Heroku Postgres Backup to S3 Utility.
Uses the Heroku PG-Backups system to pull the latest backup for a database, and put the file on Amazon S3.
Unfortunately, depends on the heroku toolbelt, since there is no standard API for PGBackups (that we have found).
Be sure that you are logged in to the heroku toolbelt before you run this script, and that it is in your $PATH.

Usage:
herokupostgres_s3_backup.py  -r <path_to_heroku> -a <app_name>  -b <bucket> -k <aws_key_id> -s <aws_secret> -p <s3_key_prefix>
herokupostgres_s3_backup.py (-h | --help)

Options:
-h --help      Show this screen.
-a <app_name> --app=<app_name>                     Heroku App name.
-r <path_to_heroku> --herokupath=<path_to_heroku>  location where the heroku executable lives, needs trailing slash
-b <bucket> --bucket=<bucket>                      S3 Bucket name
-k <aws_key_id> --awskey=<aws_key_id>              AWS Key ID
-s <aws_secret> --awssecret=<aws_secret>           AWS Secret Key
-p <s3_key_prefix> --prefix=<s3_key_prefix         Prefixes filename of S3 object
"""
import requests
import math
import os
import sys
import datetime
import subprocess
from docopt import docopt
import boto
from filechunkio import FileChunkIO


# Gets the latest backup for a given app
# Relies on the heroku cli toolbelt to talk to PGBackups
def get_backup(heroku_path, app_name):
    # first, get the heroku pgbackups:url from the heroku toolbelt
    print 'Looking up backup URL for:{0}'.format(app_name)
    #'Shelling out' isn't ideal in this situation, but it is the path of least resistance for now.
    backup_url = subprocess.check_output(heroku_path + 'heroku pgbackups:url --app {0}'.format(app_name), shell=True).rstrip()
    # download the file to disk. Stream, since the file could potentially be large
    print 'Downloading backup from:{0}'.format(backup_url)
    #We need to timestamp our own, since the backup url just gets the 'latest'
    backup_filename = app_name + '-' + datetime.datetime.now().isoformat()
    r = requests.get(backup_url, stream=True)
    with open(backup_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()
    print 'saved backup to file: {0}'.format(backup_filename)
    return backup_filename


# Using S3 Multipart upload to handle potentially large files
def upload_to_s3(s3key, filename, bucket, aws_key, aws_secret):
    conn = boto.connect_s3(aws_key, aws_secret)
    bucket = conn.get_bucket(bucket)
    # Get file info
    source_path = filename
    source_size = os.stat(source_path).st_size
    # Create a multipart upload request
    mp = bucket.initiate_multipart_upload(s3key)
    # Use a chunk size of 50 MiB
    chunk_size = 52428800
    chunk_count = int(math.ceil(source_size / chunk_size))
    # Send the file parts, using FileChunkIO to create a file-like object
    # that points to a certain byte range within the original file. We
    # set bytes to never exceed the original file size.
    for i in range(chunk_count + 1):
        print 'Uploading file chunk: {0} of {1}'.format(i + 1, chunk_count + 1)
        offset = chunk_size * i
        bytes = min(chunk_size, source_size - offset)
        with FileChunkIO(source_path, 'r', offset=offset, bytes=bytes) as fp:
            mp.upload_part_from_file(fp, part_num=i + 1)
    # Finish the upload
    completed_upload = mp.complete_upload()
    return completed_upload


def delete_local_backup_file(filename):
    print 'Deleting file from local filesystem:{0}'.format(filename)
    os.remove(filename)


if __name__ == '__main__':
    # grab all the arguments
    arguments = docopt(__doc__, version='herokupostgres_s3_backup 0.0.1')
    app_name = arguments['--app']
    heroku_path = arguments['--herokupath']
    bucket = arguments['--bucket']
    aws_key = arguments['--awskey']
    aws_secret = arguments['--awssecret']
    prefix = arguments['--prefix']

    # first, fetch the backup
    filename = get_backup(heroku_path, app_name)
    if not filename:
        # we failed to save the backup successfully.
        sys.exit(1)
    # now, store the file we just downloaded up on S3
    print 'Uploading file to S3. Bucket:{0}'.format(bucket)
    s3_success = upload_to_s3(prefix + filename, filename, bucket, aws_key, aws_secret)
    if not s3_success:
        # somehow failed the file upload
        print 'Failure with S3 upload. Exiting...'
        sys.exit(1)
    print 'Upload to S3 completed successfully'
    # Delete the local backup file, to not take up excessive disk space
    delete_local_backup_file(filename)
