#!/usr/bin/env python

import os
import sys
import json
import uuid
import gfal2
import errno
import argparse
import logging
import requests
import itertools
import fts3.rest.client.easy as fts3
import fts3.rest.client.exceptions as fts3_client_exceptions
from datetime import datetime

# CONFIG VARIABLES
FILE_PREFIX = "fts.testfile"
FTS_ENDPOINT = "https://fts3-pilot.cern.ch:8446"
DEFAULT_LOCALPATH = "/tmp/ridona/temp_files_fts"
LOCALPATH_TEMP_DIR = os.getenv("LOCALPATH", DEFAULT_LOCALPATH)
MB = 1048576

# ------------------------------------------------------------------------------


def _gfal_clean_up_dir(directory, hours=24):
    """
    Remove all files from a directory

    Args:
        directory(str): Directory path

    Returns:
        None if successful
        -1 if error
    """
    logger = logging.getLogger()
    context = gfal2.creat_context()

    filenames = context.listdir(str(directory))
    if filenames:
        logger.info('gfal-ls (x{}) {}'.format(len(filenames), directory))
        logger.handlers[0].flush()

        actually_deleted = 0
        for file in filenames:
            gfal_file = os.path.join(directory, file)
            try:
                info = context.lstat(str(gfal_file))
                file_time = datetime.fromtimestamp(info.st_mtime)
                now = datetime.now()
                diff_time = now - file_time
                if diff_time.seconds / 60 / 60 > hours:
                    error = context.unlink(str(gfal_file))
                    if not error:
                        actually_deleted += 1
                    else:
                        logger.info("error:{}").format(error)
            except Exception as e:
                logger.info("gfal-rm failed:{}, gfal_file:{}".format(
                    e, gfal_file))
                logger.handlers[0].flush()
                continue

        logger.info('gfal-rm (x{} | hours={}) {}'.format(
            actually_deleted, hours, directory))
        logger.handlers[0].flush()
    return None


def _gfal_rm_files(filenames, directory):
    """
    Remove files from a directory

    Args:
        filenames(list): List of filenames
        directory(str): Directory path

    Returns:
        None if successful
        -1 if error
    """
    logger = logging.getLogger()
    context = gfal2.creat_context()

    logger.info('gfal-rm (x{}) {}'.format(len(filenames), directory))
    logger.handlers[0].flush()
    for file in filenames:
        gfal_file = os.path.join(directory, file)
        try:
            error = context.unlink(str(gfal_file))
            if not error:
                pass
            else:
                logger.info("error:{}").format(error)
        except Exception as e:
            logger.info("gfal-rm failed:{}, gfal_file:{}".format(e, gfal_file))
            logger.handlers[0].flush()
            return -1
    return None


def _gfal_upload_files(local_file_paths, directory, filenames):
    """
    Upload files to a directory

    Args:
        directory(str): Directory path
        local_file_paths(list): List of local files paths that will be uploaded
        filenames(list): List of filenames that will be uploaded

    Returns:
        None if successful
        -1 if error

    """

    logger = logging.getLogger()
    context = gfal2.creat_context()

    # set transfer parameters
    params = context.transfer_parameters()
    params.overwrite = False
    params.checksum_check = False

    sources = []
    destinations = []
    for i in xrange(len(local_file_paths)):
        filename = filenames[i]
        local_file_path = local_file_paths[i]
        gfal_file = "file://" + local_file_path
        sources.append(str(gfal_file))
        destinations.append(str(os.path.join(directory, filename)))

    logger.info('gfal-copy (x{}) {}'.format(len(sources), directory))
    logger.handlers[0].flush()
    try:
        for i in xrange(len(sources)):
            src = sources[i]
            dst = destinations[i]
            error = context.filecopy(params, src, dst)
            if not error:
                pass
                # logger.info("{} => {} succeeded!".format(src, dst))
            else:
                logger.info("{} => {} failed [{}] {}".format(
                    src, dst, error.code, error.message))
                logger.handlers[0].flush()
                return -1
    except Exception as e:
        logger.info("Copy failed: {}".format(e))
        logger.handlers[0].flush()
        return -1

    return None


def _gfal_setup_folders(endpnt_list, testing_folder, cleanup=False):
    """
    Setup folders at endpoint

    Args:
        endpnt_list(str): List of endpoints to setup folders at
        testing_folder(str): Folder name to remove/create
    Returns: None
    """
    logger = logging.getLogger()
    context = gfal2.creat_context()

    problematic_endpoints = []

    # for each endpoint
    for endpnt in endpnt_list:
        # list directories/files
        logger.info('gfal-ls {}'.format(endpnt))
        logger.handlers[0].flush()
        try:
            dir_names = context.listdir(endpnt)
        except Exception as e:
            logger.info("gfal-ls failed:{}, endpoint:{}".format(e, endpnt))
            logger.handlers[0].flush()
            problematic_endpoints.append(endpnt.split("://")[1])
            continue

        base_dir = os.path.join(endpnt, testing_folder)
        src_dir = os.path.join(endpnt, testing_folder, "src")
        dest_dir = os.path.join(endpnt, testing_folder, "dest")

        # if folder does not exist
        if testing_folder not in dir_names:
            # create folder
            logger.info('gfal-mkdir {}'.format(base_dir))
            logger.handlers[0].flush()
            try:
                context.mkdir(str(base_dir), 0775)
                context.mkdir(str(src_dir), 0775)
                context.mkdir(str(dest_dir), 0775)
            except Exception as e:
                logger.info("gfal-mkdir failed:{}, dir:{}".format(e, base_dir))
                logger.handlers[0].flush()
                problematic_endpoints.append(endpnt)
                continue
        else:
            dir_names = context.listdir(str(base_dir))
            if "src" not in dir_names:
                logger.info('gfal-mkdir {}'.format(src_dir))
                logger.handlers[0].flush()
                context.mkdir(str(src_dir), 0775)
            if "dest" not in dir_names:
                logger.info('gfal-mkdir {}'.format(dest_dir))
                logger.handlers[0].flush()
                context.mkdir(str(dest_dir), 0775)

        if cleanup:
            _gfal_clean_up_dir(dest_dir, hours=6)
            # _gfal_clean_up_dir(src_dir)

    return problematic_endpoints


def _gfal_check_files(directory, filesize, numfile):
    """
    """

    logger = logging.getLogger()
    context = gfal2.creat_context()

    return_filenames = []
    filenames = context.listdir(str(directory))
    if filenames:
        # we have files
        if len(filenames) < numfile:
            # we have less files than we want
            return []
        for file in filenames:
            if file.endswith("{}mb".format(filesize)):
                return_filenames.append(file)
        if len(return_filenames) < numfile:
            # we have less files of filesize than we want
            return []
    else:
        # no files at all
        return []

    return return_filenames


# ------------------------------------------------------------------------------


def _fts_poll_job(context, job_id):
    """
    """
    logger = logging.getLogger()
    try:
        while True:
            response = json.loads(context.get("/jobs/" + job_id))
            if response['http_status'] == "200 Ok":
                if response["job_finished"]:
                    logger.info(
                        'Job with id {} finished with job_state:{}'.format(
                            job_id, response['job_state']))
                    logger.handlers[0].flush()
                    break
            else:
                logger.info('Server http status: {}'.format(
                    response['http_status']))
                logger.handlers[0].flush()
                return None
    except Exception as e:
        logger.info("Polling failed:{}, response:{}".format(e, response))
        logger.handlers[0].flush()
        return None

    return response['job_state']


def _fts_wait_jobs(context, job_map_list):
    """
    """
    logger = logging.getLogger()
    finished_jobs = []
    try:
        while len(finished_jobs) < len(job_map_list):
            for job_map in job_map_list:
                job_id = job_map['job_id']
                if job_id in finished_jobs:
                    continue
                response = fts3.get_job_status(context, job_id, list_files=True)
                if response['http_status'] == "200 Ok":
                    if response["job_finished"]:
                        finished_jobs.append(job_id)
                        logger.info(
                            'Job with id {} finished with job_state:{} | {}/{}'.
                            format(job_id, response['job_state'],
                                   len(finished_jobs), len(job_map_list)))
                        logger.handlers[0].flush()

                        if response['job_state'] == "FINISHED":
                            _gfal_rm_files(job_map['files_to_purge'],
                                           job_map['directory'])
                        else:
                            filenames = []
                            for file_map in response['files']:
                                if file_map['file_state'] == 'FINISHED':
                                    filenames.append(
                                        file_map['dest_surl'].split(
                                            "/dest/")[1])
                            _gfal_rm_files(filenames, job_map['directory'])
                        break
                else:
                    logger.info('Server http status: {}'.format(
                        response['http_status']))
                    logger.handlers[0].flush()

    except Exception as e:
        logger.info("Polling failed:{}, response:{}".format(e, response))
        logger.handlers[0].flush()
        return None

    return None


def _fts_submit_job(source_url, dest_url, src_filenames, dst_filenames,
                    checksum, overwrite, testing_folder, context, metadata):
    """
    https://gitlab.cern.ch/fts/fts-rest/-/blob/develop/src/fts3/rest/client/easy/submission.py#L106
    """

    logger = logging.getLogger()

    transfers = []
    for i in xrange(len(src_filenames)):
        source_file = os.path.join(source_url, testing_folder, "src",
                                   src_filenames[i])
        dest_file = os.path.join(dest_url, testing_folder, "dest",
                                 dst_filenames[i])
        transfer = fts3.new_transfer(source=source_file, destination=dest_file)
        transfers.append(transfer)

    # create job
    job = fts3.new_job(transfers,
                       verify_checksum=checksum,
                       overwrite=overwrite,
                       timeout=3600,
                       metadata=metadata)

    # submit job
    while True:
        try:
            job_id = fts3.submit(context, job)
            break
        except fts3_client_exceptions.ClientError as e:
            logger.info(e)
            logger.handlers[0].flush()

    return job_id


# ------------------------------------------------------------------------------


def main():

    parser = argparse.ArgumentParser(description="Run FTS Datalake Tests")

    parser.add_argument("-i",
                        required=True,
                        dest="conf_file",
                        help="Configuration file")
    parser.add_argument("--cleanup",
                        required=False,
                        action='store_true',
                        default=False,
                        help="Clean up src/dst directories")

    arg = parser.parse_args()
    conf_file = str(arg.conf_file)
    cleanup = arg.cleanup

    logging.basicConfig(format='%(asctime)s %(message)s',
                        datefmt='%d/%m/%Y %I:%M:%S %p',
                        level=logging.INFO)
    logging.getLogger("gfal2").setLevel(logging.WARNING)
    logger = logging.getLogger()

    with open(conf_file) as json_file:
        data = json.load(json_file)

        # assign json variables
        protocol_map = data['protocols']
        num_of_files_list = data['num_of_files']
        filesize_list = data['filesizes']
        num_of_jobs = data['num_of_jobs']
        testing_folder = data['testing_folder']
        checksum = data["checksum"]
        overwrite = data["overwrite"]
        metadata = data['metadata']

        # setup folders at the testing endpoints if needed
        endpoints = []
        endpoint_tlist = []
        for protocol in protocol_map:
            protocol_endpoints = protocol_map[protocol]
            for endpoint in protocol_endpoints:
                endpoint_t = endpoint.split(":", 1)[0]
                if endpoint_t not in endpoint_tlist:
                    endpoint_tlist.append(endpoint_t)
                    endpoints.append("{}://{}".format(protocol, endpoint))
        del endpoint_tlist
        prob_endpoints = _gfal_setup_folders(endpoints, testing_folder, cleanup)
        if prob_endpoints:
            logger.info(
                "Problematic endpoints (will not be tested): {})".format(
                    prob_endpoints))
            logger.handlers[0].flush()

        # if cleanup:
        #     sys.exit(1)

        # authenticate @ FTS endpoint
        # https://gitlab.cern.ch/fts/fts-rest/-/blob/develop/src/fts3/rest/client/context.py#L148
        logger.info('Authenticating at {}'.format(FTS_ENDPOINT))
        logger.handlers[0].flush()
        context = fts3.Context(FTS_ENDPOINT, verify=True)

        job_map_list = []

        # for every job
        for _ in xrange(num_of_jobs):
            for protocol in protocol_map:
                protocol_endpoints = protocol_map[protocol]
                endpnt_pairs = itertools.permutations(protocol_endpoints, 2)
                for endpnt_pair in endpnt_pairs:
                    abort_source = False
                    source_url = "{}://{}".format(protocol, endpnt_pair[0])
                    dest_url = "{}://{}".format(protocol, endpnt_pair[1])
                    if endpnt_pair[0] in prob_endpoints or endpnt_pair[
                            1] in prob_endpoints:
                        continue
                    logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
                    logger.info("Source: {}".format(source_url))
                    logger.info("Destination: {}".format(dest_url))
                    logger.handlers[0].flush()
                    # for every filesize combination
                    for filesize in filesize_list:
                        if abort_source:
                            break

                        # for every files per job combination
                        for numfile in num_of_files_list:

                            # configure destination filenames
                            local_file_paths = []
                            dest_filenames = []
                            for nfile in xrange(numfile):
                                random_suffix = str(uuid.uuid1())
                                random_filename = "{}.{}".format(
                                    FILE_PREFIX, random_suffix)
                                dest_filenames.append(random_filename)
                                file_path = os.path.join(
                                    LOCALPATH_TEMP_DIR, random_filename)
                                local_file_paths.append(str(file_path))

                            source_dir = os.path.join(source_url,
                                                      testing_folder, "src")

                            # check if source has adequate number of files of
                            # the desired filesize
                            logger.info(
                                "Checking source for {} existing {}MB files".
                                format(numfile, filesize))
                            logger.handlers[0].flush()
                            src_filenames = _gfal_check_files(
                                source_dir, filesize, numfile)

                            remove_local_files = False
                            if not src_filenames:
                                remove_local_files = True
                                for filename in dest_filenames:
                                    src_filename = "{}_{}mb".format(
                                        filename, filesize)
                                    src_filenames.append(src_filename)

                                # generate random files localy
                                logger.info(
                                    "Generating {} random files of size:{}MB".
                                    format(numfile, filesize))
                                logger.handlers[0].flush()
                                for file_path in local_file_paths:
                                    with open(file_path, 'wb') as fout:
                                        fout.write(os.urandom(filesize * MB))

                                # upload files to the source for this job
                                logger.info("Uploading files to source")
                                logger.handlers[0].flush()
                                rcode = _gfal_upload_files(
                                    local_file_paths, source_dir, src_filenames)
                                if rcode == -1:
                                    abort_source = True
                                    break

                            # submit fts transfer
                            logger.info('Submitting FTS job')
                            logger.handlers[0].flush()
                            job_id = _fts_submit_job(source_url, dest_url,
                                                     src_filenames,
                                                     dest_filenames, checksum,
                                                     overwrite, testing_folder,
                                                     context, metadata)
                            logger.info('FTS job id:{}'.format(job_id))
                            logger.handlers[0].flush()

                            job_map = {}
                            job_map['job_id'] = job_id
                            job_map['directory'] = os.path.join(
                                dest_url, testing_folder, "dest")
                            job_map['files_to_purge'] = dest_filenames
                            job_map_list.append(job_map)

                            if remove_local_files:
                                # remove files locally
                                logger.info(
                                    "Removing files from LOCALPATH: {}".format(
                                        LOCALPATH_TEMP_DIR))
                                logger.handlers[0].flush()
                                for file in local_file_paths:
                                    os.remove(file)

        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        logger.handlers[0].flush()
        _fts_wait_jobs(context, job_map_list)


if __name__ == '__main__':
    main()
