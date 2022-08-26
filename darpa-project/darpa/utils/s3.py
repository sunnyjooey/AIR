import logging
import os
import shutil
import tempfile
from contextlib import contextmanager
from urllib.parse import urlparse

import boto3

logger = logging.getLogger("luigi-interface")


@contextmanager
def download_s3_resource(s3_resource):
    """Context manager that downloads resource from S3 to local filesystem.

    It should work for single files as well as directories.
    It should work for luigi Targets as well as S3 URLs.

    So first we have to determine whether we deal with a target or URL,
    then whether it's a single file or directory.
    A target will have a `path` attribute, that will just return the URL.
    """
    if hasattr(s3_resource, "path"):
        s3_resource = s3_resource.path
    # we should deal with a URL now in any case
    s3 = boto3.resource("s3")
    s3_components = urlparse(s3_resource)
    bucket_name = s3_components.netloc
    bucket = s3.Bucket(bucket_name)
    # remove the leading slash, we need a relative path
    s3_location = s3_components.path[1:]
    # not using NamedDirectory as context manager, we don't want the
    # directory to be deleted when this context ends, but later
    tmpdir = tempfile.TemporaryDirectory().name
    os.makedirs(tmpdir, exist_ok=True)

    if s3_location.endswith("/"):
        # we are dealing with a directory in S3 (prefix).

        # we can download the folder by filtering by prefix,
        # see https://stackoverflow.com/a/62945526/204706
        for obj in bucket.objects.filter(Prefix=s3_location):
            target = os.path.join(tmpdir, os.path.relpath(obj.key, s3_location))
            # create subdirectories
            if not os.path.exists(os.path.dirname(target)):
                os.makedirs(os.path.dirname(target))
            if obj.key[-1] == "/":
                continue
            bucket.download_file(obj.key, target)
        downloaded_s3_resource = tmpdir
    else:
        target = os.path.join(tmpdir, os.path.basename(s3_location))
        bucket.download_file(s3_location, target)
        downloaded_s3_resource = target

    yield downloaded_s3_resource

    # remove the file/folder when with block exits
    try:
        shutil.rmtree(downloaded_s3_resource)
    except NotADirectoryError:
        # this should be safe, we created the temporary directory above
        shutil.rmtree(os.path.dirname(downloaded_s3_resource))
