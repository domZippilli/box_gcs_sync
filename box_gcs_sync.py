#!/usr/bin/env python3
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Utility to synchronize a Box account's files with a GCS bucket.
"""
import argparse
import json
import logging
import warnings
from concurrent.futures import (Executor, Future, ThreadPoolExecutor,
                                as_completed)
from datetime import datetime
from functools import wraps
from io import BytesIO
from multiprocessing import cpu_count
from typing import BinaryIO, Callable, Iterable, List

from boxsdk import Client as BoxClient
from boxsdk import JWTAuth
from boxsdk.exception import BoxAPIException
from boxsdk.object.metadata_template import (MetadataField, MetadataFieldType,
                                             MetadataTemplate)
from google.cloud import kms_v1
from google.cloud.storage import Blob, Bucket
from google.cloud.storage import Client as GCSClient

warnings.filterwarnings(
    "ignore", "Your application has authenticated using end user credentials")

logging.basicConfig()
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)

BOX_MTIME_KEY = "box_mtime"


def json_config_to_jwtauth(plaintext: str) -> dict:
    """Parse a Box application configuration JSON file into JWTAuth arguments.
    
    Arguments:
        filename {str} -- The Box app configuration JSON file.
    
    Returns:
        dict -- Arguments for the JWTAuth constructor. 
    """
    config = json.loads(plaintext)
    return {
        "client_id":
        config["boxAppSettings"]["clientID"],
        "client_secret":
        config["boxAppSettings"]["clientSecret"],
        "enterprise_id":
        config["enterpriseID"],
        "jwt_key_id":
        config["boxAppSettings"]["appAuth"]["publicKeyID"],
        "rsa_private_key_data":
        config["boxAppSettings"]["appAuth"]["privateKey"],
        "rsa_private_key_passphrase":
        config["boxAppSettings"]["appAuth"]["passphrase"]
    }


def get_box_client(plaintext: str) -> BoxClient:
    auth_args = json_config_to_jwtauth(plaintext)
    auth = JWTAuth(**auth_args)
    access_token = auth.authenticate_instance()
    return BoxClient(auth)


def impersonate_mirror_user(box: BoxClient, login: str) -> BoxClient:
    mirror_user = [x for x in box.users() if x.login == login][0]
    LOG.info("Mirroring user: {}, login: {}".format(mirror_user,
                                                    mirror_user.login))
    return box.as_user(mirror_user)


def box_walk(folder) -> Iterable[tuple]:
    """Walks a Box folder, recursively returning all files and folders in the starting point.

    Each item will be a tuple consisting of the path to the file/folder, and its corresponding Box SDK object.
    
    Arguments:
        folder {Folder} -- A Box folder object
    
    Yields:
        (str, object) -- A string with a path to the file/folder, and an object representing the item.
    """
    files, folders = [], []
    for item in folder.get().get_items():
        if item.type == "folder":
            folders.append(item)
        else:
            yield (item.name, item)

    for fldr in folders:
        yield (fldr.name + "/", fldr)
        for item in box_walk(fldr):
            yield (fldr.name + "/" + item[0], item[1])


def box_mkdir_p(box: BoxClient, path: List[str], cache: dict):
    # first, check the cache for the path needed.
    cache_key = '/'.join(path) + '/'

    if not cache.get(cache_key, False):
        # if not found, walk the directories, making them if needed, caching each
        path_so_far = []
        current_folder = box.root_folder()

        for element in path:
            path_so_far.append(element)
            cache_key_so_far = '/'.join(path_so_far) + '/'

            if not cache.get(cache_key_so_far, False):
                # This path does not exist
                new_folder = current_folder.create_subfolder(element)
                cache[cache_key_so_far] = new_folder
            current_folder = cache[cache_key_so_far]

    # return the cached entry
    return cache[cache_key]


def concurrent_upload(reader_func: Callable,
                      writer_func: Callable,
                      temp_file: BinaryIO,
                      callback: Callable = None) -> None:
    """Perform a download into temp_file, and then an upload from it.
    
    Arguments:
        reader_func {Callable} -- A function that will read bytes into the temp_file from remote storage.
        writer_func {Callable} -- A function that will read bytes from the temp_file into remote storage.
        temp_file {BinaryIO} -- A temp_file to use. The download will complete in this file before the upload begins.

    Keyword Arguments:
        callback {Callable} -- A function that will be invoked with the return value of the writer_func. (default: {None})

    Returns:
        The result of writer_func(temp_file), or the result of callable(writer_func(temp_file)) if callback is provided.
    """
    reader_func(temp_file)
    temp_file.seek(0)
    if callback:
        return callback(writer_func(temp_file))
    return writer_func(temp_file)


def should_copy_box_to_gcs(box_file, box_mtime, blob,
                           blob_exists: bool) -> bool:
    if not blob_exists:
        # Always copy in this case.
        return True
    # The blob exists in all the following cases.
    if not blob.metadata or not blob.metadata[BOX_MTIME_KEY]:
        # Blob was put there by something else.
        LOG.warning(
            "Box file {} already exists in GCS, but was not synced. Skipping.".
            format(box_file.get().name))
        return False
    if datetime.fromisoformat(
            blob.metadata[BOX_MTIME_KEY]) < datetime.fromisoformat(box_mtime):
        # File was updated in Box
        return True
    # File exists in both places, was synced, and hasn't been modified since.
    return False


def sync_box_to_gcs(box: BoxClient, bucket: Bucket,
                    cache: dict) -> List[Future]:
    """Sync Box account files to a GCS bucket.

    For versioned Box files, the latest version will always be synced back to the GCS bucket. 
    Non-current versions will not be deliberately preserved, though syncing to a versioned 
    bucket will have this effect.
    
    Arguments:
        box {BoxClient} -- [description]
        bucket {Bucket} -- [description]
        cache {dict} -- A dictionary that will opportunistically be filled with Box item paths/objects.
    
    Returns:
        List[Future] -- [description]
    """
    # constuct an executor for copy tasks
    executor = ThreadPoolExecutor(max_workers=cpu_count())
    futures = []
    # sync box files back to GCS
    for path, item in box_walk(box.root_folder()):
        LOG.debug("Box directory walk found: {}".format(path))
        # cache the Box item in module scope
        cache[path] = item

        # get the blob to overwrite, or make a new one
        blob_exists = True
        blob = bucket.get_blob(path)
        if not blob:
            blob_exists = False
            blob = Blob(path, bucket)

        # branch depending on whether file or folder
        if item.type == "folder":
            if not blob_exists:
                blob.metadata = {
                    # Not an important value.
                    BOX_MTIME_KEY: datetime.now().isoformat()
                }
                # create directory marker, used by UI and FUSE
                LOG.info("Creating directory marker in GCS: {}".format(
                    blob.name))
                blob.upload_from_string(b'')

        elif item.type == "file":
            box_file = box.file(item.id)
            box_mtime = box_file.get().modified_at
            if should_copy_box_to_gcs(box_file, box_mtime, blob, blob_exists):
                LOG.info(
                    "Box file {} is not found in GCS or updated since last sync. Copying to {}."
                    .format(item.name, blob.name))
                blob.metadata = {
                    BOX_MTIME_KEY: box_mtime
                }  # This change will "follow" the upload into GCS
                temp_file = BytesIO()
                reader = box_file.download_to
                writer = blob.upload_from_file

                future = executor.submit(concurrent_upload, reader, writer,
                                         temp_file)
                futures.append(future)

        else:
            LOG.info("Ignoring item of type {}".format(item.type))

    return futures


def patch_blob_metadata(bucket, blob_name, box_file):
    blob = bucket.get_blob(blob_name)
    LOG.debug(
        "Patching blob {} metadata with mtime of uploaded Box file: {} modified at: {}"
        .format(blob, box_file, box_file.modified_at))
    blob.metadata = {BOX_MTIME_KEY: box_file.modified_at}
    LOG.debug(blob.patch())


def sync_gcs_to_box(bucket: Bucket, box: BoxClient,
                    cache: dict) -> List[Future]:
    # constuct an executor for copy tasks
    executor = ThreadPoolExecutor(max_workers=cpu_count())
    futures = []

    for blob in bucket.list_blobs():
        if cache.get(blob.name, False):
            # Found the blob in Box
            LOG.debug("Blob {} already in Box.".format(blob.name))

        else:
            # Did not find the Blob in box
            if blob.metadata and blob.metadata[BOX_MTIME_KEY]:
                LOG.info(
                    "Found blob {} in bucket that was synced, but no longer exists in Box. Deleting."
                    .format(blob.name))
                blob.delete()

            else:
                if blob.name[-1] == '/':
                    LOG.info(
                        "Found new folder {} not in Box. Creating.".format(
                            blob.name))
                    path = blob.name.split("/")[:-1]
                    # do this serially, as there should be few.
                    # Ideally, box_mkdir_p never misses cache when making files as the folder will sort first
                    box_mkdir_p(box, path, cache)
                else:
                    # Found a file that doesn't seem to be in Box.
                    blob_name = blob.name
                    LOG.info("Found new blob {} not in Box. Uploading.".format(
                        blob_name))
                    # split name by slashes; last item is file, the previous are folders
                    tokens = blob.name.split("/")
                    path, filename = tokens[:-1], tokens[-1]
                    target_folder = box_mkdir_p(box, path, cache)
                    # prepare the copy
                    temp_file = BytesIO()
                    reader = blob.download_to_file
                    writer = lambda temp: target_folder.upload_stream(
                        temp, filename)
                    transfer_callback = lambda bf: patch_blob_metadata(
                        bucket, blob_name, bf)
                    # submit the copy work
                    future = executor.submit(concurrent_upload, reader, writer,
                                             temp_file, transfer_callback)
                    futures.append(future)

    return futures


def get_exceptions(futures: Iterable[Future]) -> Iterable[Exception]:
    for future in as_completed(futures):
        if future.exception():
            yield future.exception()


def get_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""
    Synchronize a Box user's files with a GCS bucket. This utility will walk a Box account's file heirarchy and compare 
    the files with GCS blobs in the bucket. Then it will perform the reverse, listing the bucket's blobs and 
    and comparing them to the Box listing.

    Synchronization rules:
        - The Box account is considered to be authoritative; files in it will not be deleted or overwritten.
        - When a Box file is found that does not exist in GCS, it will be copied to GCS. Same for folders.
        - When a GCS blob is found that does not exist in Box, it will be copied to Box. Same for folders.
        - When a file in Box is modified, the modified file will be copied to GCS.
        - When a file in Box is deleted that was previously synced to GCS, the GCS object will be deleted.
        - When a file is found in both Box and GCS with no sync metadata, no action is taken but a warning is generated.
    """)
    # TODO this should be encrypted with KMS
    parser.add_argument(
        '-c',
        '--config',
        type=str,
        required=True,
        help=
        "Box app configuration ciphertext file path (this must be encrypted a key present in with Google Cloud KMS)."
    )
    parser.add_argument(
        '-k',
        '--keyname',
        type=str,
        required=True,
        help=
        "Fully qualified name of the key to decrypt the configuration ciphertext, as given by `gcloud kms keys list`. "
        "Should be in the format projects/[PROJECT]/locations/[LOCATION]/keyRings/[KEYRING]/cryptoKeys/[KEY]"
    )
    parser.add_argument('-l',
                        '--login',
                        type=str,
                        required=True,
                        help="Box user login (user@domain) to impersonate.")
    parser.add_argument('-b',
                        '--bucket',
                        type=str,
                        required=True,
                        help="GCS bucket to synchronize.")
    return parser.parse_args()


def main():
    args = get_args()
    # Log in to Box
    LOG.info("Authenticating with Box and impersonating user {}.".format(
        args.login))

    kms = kms_v1.KeyManagementServiceClient()
    config_ciphertext = open(args.config, 'rb').read()
    config_plaintext = kms.decrypt(args.keyname, config_ciphertext).plaintext

    box = impersonate_mirror_user(get_box_client(config_plaintext), args.login)
    # Log in to GCS and get bucket
    bucket_name = args.bucket.replace("gs://", "")
    LOG.info(
        "Authenticating with GCS and fetching bucket {}.".format(bucket_name))
    bucket = GCSClient().get_bucket(bucket_name)
    # Walk Box, schedule async copies to GCS, and get a list of new blobs.
    # We will also opportunistically form a cache of the Box items.
    LOG.info("Walking Box directories and copying to GCS as needed.")
    box_cache = {'/': box.root_folder()}
    copy_jobs = sync_box_to_gcs(box, bucket, cache=box_cache)
    # Check for and log exceptions. Doing this here also institutes a "pause" between the two sync phases.
    for exc in get_exceptions(copy_jobs):
        LOG.exception(exc)
    # Walk GCS, checking against the cache of Box items, and as needed, schedule async uploads to Box
    LOG.info("Listing GCS blobs and looking for blobs to upload or delete.")
    copy_jobs = sync_gcs_to_box(bucket, box, cache=box_cache)
    # Check for and log exceptions
    for exc in get_exceptions(copy_jobs):
        LOG.exception(exc)
    LOG.info("Synchronization complete.")


if __name__ == "__main__":
    exit(main())
