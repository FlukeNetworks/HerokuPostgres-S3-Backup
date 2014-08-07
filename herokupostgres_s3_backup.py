#! /usr/bin/env python
"""Heroku Postgres Backup to S3 Utility.
Uses the Heroku PG-Backups system to pull the latest backup for a database, and put the file on Amazon S3

Usage:
herokupostgres_s3_backup.py -n <backup_filename_prefix> -u <heroku_postgres_download_url> -b <bucket> -k <aws_key_id> -s <aws_secret> -p <s3_key_prefix>
herokupostgres_s3_backup.py (-h | --help)

Options:
-h --help      Show this screen.
-u <heroku_postgres_download_url> --url=<heroku_postgres_download_url>  PG-Backups url from Heroku. Find this by running `heroku pgbackups:url`
-n <backup_filename_prefix> --name=<backup_filename_prefix>      Prefix for the output filename of the backup
-b <bucket> --bucket=<bucket>                  S3 Bucket name
-k <aws_key_id> --awskey=<aws_key_id>          AWS Key ID
-s <aws_secret> --awssecret=<aws_secret>       AWS Secret Key
-p <s3_key_prefix> --prefix=<s3_key_prefix     Prefixes filename of S3 object [default: '']
"""
import requests
import math
import os
import sys
import datetime
from docopt import docopt
import boto
from filechunkio import FileChunkIO


# Gets the latest backup for a given database and account.
def get_backup(backup_url, backup_prefix):
    # download the file to disk. Stream, since the file could potentially be large
    print 'Downloading Backup from:{0}'.format(backup_url)
    #We need to timestamp our own, since the backup url just gets the 'latest'
    backup_filename = backup_prefix + '-' + datetime.datetime.now().isoformat()
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
    download_url = arguments['--url']
    backup_prefix = arguments['--name']
    bucket = arguments['--bucket']
    aws_key = arguments['--awskey']
    aws_secret = arguments['--awssecret']
    prefix = arguments['--prefix']

    # first, fetch the backup
    filename = get_backup(download_url, backup_prefix)
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
