#!/usr/bin/env python

import os
import re
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
LOCALPATH_TEMP_DIR = os.getenv("FTS_LOCALPATH", DEFAULT_LOCALPATH)
MB = 1048576

# ------------------------------------------------------------------------------

logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%d/%m/%Y %I:%M:%S %p',
                    level=logging.INFO)
logging.getLogger("gfal2").setLevel(logging.WARNING)
logger = logging.getLogger()


def _flush_logging_msg(msg):
    """
    """
    logger.info(msg)
    logger.handlers[0].flush()


# ------------------------------------------------------------------------------


def _gfal_clean_up_dir(directory, hours=24, timeout=300):
    """
    Remove all files from a directory

    Args:
        directory(str): Directory path

    Returns:
        None if successful
        -1 if error
    """
    context = gfal2.creat_context()
    params = context.transfer_parameters()
    params.timeout = timeout

    _flush_logging_msg('gfal-ls {}'.format(directory))
    try:
        filenames = context.listdir(params, str(directory))
    except Exception as e:
        _flush_logging_msg("gfal-ls failed:{}, endpoint:{}".format(
            e, directory))
        return -1

    if filenames:
        _flush_logging_msg('gfal-ls (x{}) {}'.format(len(filenames), directory))

        actually_deleted = 0
        for file in filenames:
            gfal_file = os.path.join(directory, file)
            try:
                info = context.lstat(params, str(gfal_file))
                file_time = datetime.fromtimestamp(info.st_mtime)
                now = datetime.now()
                diff_time = now - file_time
                if diff_time.seconds / 60 / 60 > hours:
                    error = context.unlink(params, str(gfal_file))
                    if not error:
                        actually_deleted += 1
                    else:
                        _flush_logging_msg("error:{}").format(error)
            except Exception as e:
                _flush_logging_msg("gfal-rm failed:{}, gfal_file:{}".format(
                    e, gfal_file))
                continue

        _flush_logging_msg('gfal-rm (x{} | hours={}) {}'.format(
            actually_deleted, hours, directory))
    return None


def _gfal_rm_files(filenames, directory, timeout=300):
    """
    Remove files from a directory

    Args:
        filenames(list): List of filenames
        directory(str): Directory path

    Returns:
        None if successful
        -1 if error
    """
    context = gfal2.creat_context()
    params = context.transfer_parameters()
    params.timeout = timeout

    _flush_logging_msg('gfal-rm (x{}) {}'.format(len(filenames), directory))
    for file in filenames:
        gfal_file = os.path.join(directory, file)
        try:
            error = context.unlink(params, str(gfal_file))
            if not error:
                pass
            else:
                _flush_logging_msg("error:{}").format(error)
        except Exception as e:
            _flush_logging_msg("gfal-rm failed:{}, gfal_file:{}".format(
                e, gfal_file))
            continue
    return None


def _gfal_upload_files(local_file_paths, directory, filenames, timeout=300):
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
    # set transfer parameters
    context = gfal2.creat_context()
    params = context.transfer_parameters()
    params.overwrite = False
    params.checksum_check = True
    params.timeout = timeout

    sources = []
    destinations = []
    for i in xrange(len(local_file_paths)):
        filename = filenames[i]
        local_file_path = local_file_paths[i]
        gfal_file = "file://" + local_file_path
        sources.append(str(gfal_file))
        destinations.append(str(os.path.join(directory, filename)))

    _flush_logging_msg('gfal-copy (x{}) {}'.format(len(sources), directory))
    try:
        for i in xrange(len(sources)):
            src = sources[i]
            dst = destinations[i]
            error = context.filecopy(params, src, dst)
            if not error:
                pass
                # _flush_logging_msg("{} => {} succeeded!".format(src, dst))
            else:
                _flush_logging_msg("{} => {} failed [{}] {}".format(
                    src, dst, error.code, error.message))
                return -1
    except Exception as e:
        _flush_logging_msg("Copy failed: {}".format(e))
        return -1

    return None


def _gfal_setup_folders(endpnt_list,
                        testing_folder,
                        cleanup=False,
                        timeout=300):
    """
    Setup folders at endpoint

    Args:
        endpnt_list(str): List of endpoints to setup folders at
        testing_folder(str): Folder name to remove/create
    Returns: None
    """
    context = gfal2.creat_context()
    params = context.transfer_parameters()
    params.timeout = timeout

    problematic_endpoints = []

    # for each endpoint
    for endpnt in endpnt_list:
        endpnt_noprotocol = endpnt.split("://")[1]
        # list directories/files
        _flush_logging_msg('gfal-ls {}'.format(endpnt))
        try:
            dir_names = context.listdir(params, endpnt)
        except Exception as e:
            _flush_logging_msg("gfal-ls failed:{}, endpoint:{}".format(
                e, endpnt))
            problematic_endpoints.append(endpnt_noprotocol)
            continue

        base_dir = os.path.join(endpnt, testing_folder)
        src_dir = os.path.join(endpnt, testing_folder, "src")
        dest_dir = os.path.join(endpnt, testing_folder, "dest")

        # if folder does not exist
        if testing_folder not in dir_names:
            # create folder
            _flush_logging_msg('gfal-mkdir {}'.format(base_dir))
            try:
                context.mkdir(params, str(base_dir), 0775)
                context.mkdir(params, str(src_dir), 0775)
                context.mkdir(params, str(dest_dir), 0775)
            except Exception as e:
                _flush_logging_msg("gfal-mkdir failed:{}, dir:{}".format(
                    e, base_dir))
                problematic_endpoints.append(endpnt_noprotocol)
                continue
        else:
            try:
                dir_names = context.listdir(params, str(base_dir))
            except Exception as e:
                _flush_logging_msg("gfal-ls failed:{}, dir:{}".format(
                    e, base_dir))
                problematic_endpoints.append(endpnt_noprotocol)
                continue
            if "src" not in dir_names:
                _flush_logging_msg('gfal-mkdir {}'.format(src_dir))
                try:
                    context.mkdir(params, str(src_dir), 0775)
                except Exception as e:
                    _flush_logging_msg("gfal-mkdir failed:{}, dir:{}".format(
                        e, base_dir))
                    problematic_endpoints.append(endpnt_noprotocol)
                    continue
            if "dest" not in dir_names:
                _flush_logging_msg('gfal-mkdir {}'.format(dest_dir))
                try:
                    context.mkdir(params, str(dest_dir), 0775)
                except Exception as e:
                    _flush_logging_msg("gfal-mkdir failed:{}, dir:{}".format(
                        e, dest_dir))
                    problematic_endpoints.append(endpnt_noprotocol)
                    continue
        if cleanup:
            _flush_logging_msg(
                "Cleaning up destination folder: {}".format(dest_dir))
            _gfal_clean_up_dir(dest_dir, hours=2)

    return problematic_endpoints


def _gfal_check_files(directory, filesize, numfile, timeout=300):
    """
    """
    context = gfal2.creat_context()
    params = context.transfer_parameters()
    params.timeout = timeout

    return_filenames = []

    _flush_logging_msg('gfal-ls {}'.format(directory))
    try:
        filenames = context.listdir(params, str(directory))
    except Exception as e:
        _flush_logging_msg("gfal-ls failed:{}, endpoint:{}".format(
            e, directory))
        return -1

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
    try:
        while True:
            response = json.loads(context.get("/jobs/" + job_id))
            if response['http_status'] == "200 Ok":
                if response["job_finished"]:
                    _flush_logging_msg(
                        'Job with id {} finished with job_state:{}'.format(
                            job_id, response['job_state']))
                    break
            else:
                _flush_logging_msg('Server http status: {}'.format(
                    response['http_status']))
                return None
    except Exception as e:
        _flush_logging_msg("Polling failed:{}, response:{}".format(e, response))
        return None

    return response['job_state']


def _fts_wait_jobs(context, job_map_list):
    """
    """
    finished_jobs = []
    while len(finished_jobs) < len(job_map_list):
        for job_map in job_map_list:
            try:
                job_id = job_map['job_id']
                if job_id in finished_jobs:
                    continue
                response = fts3.get_job_status(context, job_id, list_files=True)
                if response['http_status'] == "200 Ok":
                    if response["job_finished"]:
                        finished_jobs.append(job_id)
                        _flush_logging_msg(
                            'Job with id {} finished with job_state:{} | {}/{}'.
                            format(job_id, response['job_state'],
                                   len(finished_jobs), len(job_map_list)))

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
                            _flush_logging_msg(
                                "Removing testing files from destination")
                            _gfal_rm_files(filenames, job_map['directory'])
                        break
                else:
                    _flush_logging_msg('Server http status: {}'.format(
                        response['http_status']))
                    finished_jobs.append(job_id)
                    continue
            except Exception as e:
                _flush_logging_msg("Polling failed:{}, response:{}".format(
                    e, response))
                finished_jobs.append(job_id)
                continue

    return None


def _fts_submit_job(source_url, dest_url, src_filenames, dst_filenames,
                    checksum, overwrite, testing_folder, context, metadata):
    """
    https://gitlab.cern.ch/fts/fts-rest/-/blob/develop/src/fts3/rest/client/easy/submission.py#L106
    """

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
            _flush_logging_msg(e)

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
    parser.add_argument("--exit",
                        required=False,
                        action='store_true',
                        default=False,
                        help="Exit after cleanup")

    arg = parser.parse_args()
    conf_file = str(arg.conf_file)
    cleanup = arg.cleanup
    exit = arg.exit

    # open configuration file to get test details
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

        # figure out the unique endpoints from the configuration
        endpoints = []
        endpoint_tlist = []
        for protocol in protocol_map:
            protocol_endpoints = protocol_map[protocol]
            for endpoint in protocol_endpoints:
                # example: endpoint = door05.pic.es:8452//rucio/pic_dcache
                endpoint_t = endpoint.split(":", 1)[0]
                # example: endpoint_t = door05.pic.es
                endpoint_e = re.split('[0-9]*', endpoint.split(":", 1)[1], 1)[1]
                # example: endpoint_e = //rucio/pic_dcache
                endpoint_ft = endpoint_t + endpoint_e
                if endpoint_ft not in endpoint_tlist:
                    endpoint_tlist.append(endpoint_ft)
                    endpoints.append("{}://{}".format(protocol, endpoint))
        del endpoint_tlist

        # setup folders at the testing endpoints if needed
        _flush_logging_msg("Setting up folders at endpoints")
        prob_endpoints = _gfal_setup_folders(endpoints, testing_folder, cleanup)

        # we have some problematic endpoints
        if prob_endpoints:
            _flush_logging_msg(
                "Problematic endpoints (will not be tested): {})".format(
                    prob_endpoints))

        # the script is used as a setup script so do not perform testing
        if exit:
            sys.exit(1)

        # ----------------------------------------------------------------------

        # authenticate @ FTS endpoint
        # https://gitlab.cern.ch/fts/fts-rest/-/blob/develop/src/fts3/rest/client/context.py#L148
        _flush_logging_msg('Authenticating at {}'.format(FTS_ENDPOINT))
        context = fts3.Context(FTS_ENDPOINT, verify=True)

        # list that holds a dictionary per each job
        # this is later used to poll for the jobs until they finish
        job_map_list = []

        # for every job
        for _ in xrange(num_of_jobs):
            # for every protocol to be checked
            for protocol in protocol_map:
                # get endpoints
                protocol_endpoints = protocol_map[protocol]
                # create unique pairs of 2s (source destionation)
                endpnt_pairs = itertools.permutations(protocol_endpoints, 2)
                # for every pair
                for endpnt_pair in endpnt_pairs:
                    # ad-hoc temp solution for lapp-webdav - remove checksum
                    if endpnt_pair[0] == "lapp-esc02.in2p3.fr:8001/webdav":
                        checksum = "none"
                    if endpnt_pair[1] == "lapp-esc02.in2p3.fr:8001/webdav":
                        checksum = "none"
                    # --
                    abort_source = False
                    source_url = "{}://{}".format(protocol, endpnt_pair[0])
                    dest_url = "{}://{}".format(protocol, endpnt_pair[1])
                    # if the source endpoint is faulty, abort this run
                    if endpnt_pair[0] in prob_endpoints:
                        _flush_logging_msg("Aborting run for source: {}".format(
                            endpnt_pair[0]))
                        continue
                    _flush_logging_msg(
                        ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
                    _flush_logging_msg("Source: {}".format(source_url))
                    _flush_logging_msg("Destination: {}".format(dest_url))
                    # for every filesize combination
                    for filesize in filesize_list:
                        if abort_source:
                            _flush_logging_msg(
                                "Aborting run for source: {}".format(
                                    source_url))
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
                            _flush_logging_msg(
                                "Checking source for {} existing {}MB files".
                                format(numfile, filesize))
                            src_filenames = _gfal_check_files(
                                source_dir, filesize, numfile)

                            if src_filenames == -1:
                                abort_source = True
                                break

                            remove_local_files = False
                            if not src_filenames:
                                remove_local_files = True
                                for filename in dest_filenames:
                                    src_filename = "{}_{}mb".format(
                                        filename, filesize)
                                    src_filenames.append(src_filename)

                                # generate random files localy
                                _flush_logging_msg(
                                    "Locally generating {} random files of size:{}MB"
                                    .format(numfile, filesize))
                                for file_path in local_file_paths:
                                    with open(file_path, 'wb') as fout:
                                        fout.write(os.urandom(filesize * MB))

                                # upload files to the source for this job
                                _flush_logging_msg("Uploading files to source")
                                rcode = _gfal_upload_files(
                                    local_file_paths, source_dir, src_filenames)
                                if rcode == -1:
                                    abort_source = True
                                    break

                            # submit fts transfer
                            _flush_logging_msg('Submitting FTS job')
                            job_id = _fts_submit_job(source_url, dest_url,
                                                     src_filenames,
                                                     dest_filenames, checksum,
                                                     overwrite, testing_folder,
                                                     context, metadata)
                            _flush_logging_msg('FTS job id:{}'.format(job_id))

                            job_map = {}
                            job_map['job_id'] = job_id
                            job_map['directory'] = os.path.join(
                                dest_url, testing_folder, "dest")
                            job_map['files_to_purge'] = dest_filenames
                            job_map_list.append(job_map)

                            if remove_local_files:
                                # remove files locally
                                _flush_logging_msg(
                                    "Removing files from LOCALPATH: {}".format(
                                        LOCALPATH_TEMP_DIR))
                                for file in local_file_paths:
                                    os.remove(file)

        _flush_logging_msg(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        _fts_wait_jobs(context, job_map_list)


if __name__ == '__main__':
    main()
