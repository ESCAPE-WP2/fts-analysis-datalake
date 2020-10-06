#!/usr/bin/env python

import os
import sys
import json
import uuid
import gfal2
import errno
import argparse
import datetime
import logging
import requests
import itertools
import fts3.rest.client.easy as fts3

# CONFIG VARIABLES
FILE_PREFIX = "fts.testfile"
FTS_ENDPOINT = "https://fts3-pilot.cern.ch:8446"
LOCALPATH_TEMP_DIR = os.getenv("LOCALPATH", "./temp_files_fts")
MB = 1048576

# ------------------------------------------------------------------------------


def _setup_folders(endpnt_list, testing_folder):
    """
    Setup folders at endpoint

    Args:
        endpnt_list(str): List of endpoints to setup folders at
        testing_folder(str): Folder name to remove/create
    Returns: None
    """
    logger = logging.getLogger()
    context = gfal2.creat_context()

    # for each endpoint
    for endpnt in endpnt_list:
        # list directories/files
        logger.info('gfal-ls {}'.format(endpnt))
        dir_names = context.listdir(endpnt)

        base_dir = os.path.join(endpnt, testing_folder)
        src_dir = os.path.join(endpnt, testing_folder, "src")
        dest_dir = os.path.join(endpnt, testing_folder, "dest")

        # if folder does not exist
        if testing_folder not in dir_names:
            # create folder
            logger.info('gfal-mkdir {}'.format(base_dir))
            context.mkdir(str(base_dir), 0775)
            context.mkdir(str(src_dir), 0775)
            context.mkdir(str(dest_dir), 0775)


def _gfal_rm_files(files, url):
    """
    """
    logger = logging.getLogger()
    context = gfal2.creat_context()

    logger.info('gfal-rm (x{}) {}'.format(len(files), url))
    for file in files:
        filename = file.split(LOCALPATH_TEMP_DIR + "/", 1)[1]
        gfal_file = os.path.join(url, filename)
        try:
            errors = context.unlink(str(gfal_file))
            if not errors:
                pass
            else:
                print errors
        except Exception as e:
            logger.info("gfal-rm failed:{}, gfal_file:{}".format(e, gfal_file))
            return -1
    return None


def _gfal_upload_files(files, src_endpnt, testing_folder):
    """
    Upload files to source endpoint

    Args:
        src_endpnt(str): Source endpoint where the files will be uploaded
        files(list): List of files paths that will be uploaded

    Returns:
        List of filenames without the current local absolute path or -1 if error

    """

    logger = logging.getLogger()
    context = gfal2.creat_context()

    # set transfer parameters
    params = context.transfer_parameters()
    params.overwrite = False
    params.checksum_check = False

    filenames = []
    sources = []
    destinations = []
    for file in files:
        filename = file.split(LOCALPATH_TEMP_DIR + "/", 1)[1]
        gfal_file = "file://" + file
        sources.append(str(gfal_file))
        destinations.append(
            str(os.path.join(src_endpnt, testing_folder, "src", filename)))
        filenames.append(str(filename))

    logger.info('gfal-copy (x{}) {}'.format(
        len(sources), os.path.join(src_endpnt, testing_folder, "src")))
    try:
        errors = context.filecopy(params, sources, destinations)
        if not errors:
            pass
        else:
            for i in range(len(errors)):
                e = errors[i]
                src = sources[i]
                dst = destinations[i]
                if e:
                    logger.info("%s => %s failed [%d] %s" %
                                (src, dst, e.code, e.message))
                    return -1
                else:
                    pass
                    # logger.info("%s => %s succeeded!" % (src, dst))
    except Exception as e:
        logger.info("Copy failed: %s" % str(e))
        return -1

    return filenames


# ------------------------------------------------------------------------------


def _poll_fts_job(context, job_id):
    """
    """
    logger = logging.getLogger()
    while True:
        response = json.loads(context.get("/jobs/" + job_id))
        if response['http_status'] == "200 Ok":
            if response["job_finished"]:
                logger.info('Job with id {} finished with job_state:{}'.format(
                    job_id, response['job_state']))
                break
        else:
            logger.info('Server http status: {}'.format(
                response['http_status']))
            break

    return response['job_state']


def _submit_fts_job(source_url, dest_url, filenames, checksum, overwrite,
                    testing_folder, context):
    """
    https://gitlab.cern.ch/fts/fts-rest/-/blob/develop/src/fts3/rest/client/easy/submission.py#L106
    """

    transfers = []
    for filename in filenames:
        source_file = os.path.join(source_url, testing_folder, "src", filename)
        dest_file = os.path.join(dest_url, testing_folder, "dest", filename)
        transfer = fts3.new_transfer(source=source_file, destination=dest_file)
        transfers.append(transfer)

    metadata = {}
    metadata['activity'] = "functional-testing"

    # create job
    job = fts3.new_job(transfers,
                       verify_checksum=checksum,
                       overwrite=overwrite,
                       timeout=3600,
                       metadata=metadata)

    # submit job
    job_id = fts3.submit(context, job)

    return job_id


# ------------------------------------------------------------------------------


def main():

    parser = argparse.ArgumentParser(description="Run FTS Datalake Tests")

    parser.add_argument("-i",
                        required=True,
                        dest="conf_file",
                        help="Configuration file")

    arg = parser.parse_args()
    conf_file = str(arg.conf_file)

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
        _setup_folders(endpoints, testing_folder)

        # authenticate @ FTS endpoint
        # https://gitlab.cern.ch/fts/fts-rest/-/blob/develop/src/fts3/rest/client/context.py#L148
        logger.info('Authenticating at {}'.format(FTS_ENDPOINT))
        context = fts3.Context(FTS_ENDPOINT, verify=True)

        # for every job
        for k in xrange(num_of_jobs):
            for protocol in protocol_map:
                protocol_endpoints = protocol_map[protocol]
                endpnt_pairs = itertools.permutations(protocol_endpoints, 2)
                for endpnt_pair in endpnt_pairs:
                    abort_source = False
                    source_url = "{}://{}".format(protocol, endpnt_pair[0])
                    dest_url = "{}://{}".format(protocol, endpnt_pair[1])
                    # for every filesize combination
                    for filesize in filesize_list:
                        # for every files per job combination
                        if abort_source:
                            break
                        for numfile in num_of_files_list:
                            # for every file of the job
                            files = []
                            for nfile in xrange(numfile):
                                random_suffix = str(uuid.uuid1())
                                random_filename = "{}.{}".format(
                                    FILE_PREFIX, random_suffix)
                                file = os.path.join(LOCALPATH_TEMP_DIR,
                                                    random_filename)
                                with open(file, 'wb') as fout:
                                    fout.write(os.urandom(filesize * MB))
                                files.append(str(file))
                            # upload files to the source for this job
                            filenames = _gfal_upload_files(
                                files, source_url, testing_folder)
                            if filenames == -1:
                                abort_source = True
                                break
                            # submit fts transfer
                            logger.info('Submitting FTS job')
                            job_id = _submit_fts_job(source_url, dest_url,
                                                     filenames, checksum,
                                                     overwrite, testing_folder,
                                                     context)
                            # poll for job status
                            logger.info(
                                'Polling begins for FTS job with id {}'.format(
                                    job_id))
                            job_state = _poll_fts_job(context, job_id)

                            # remove files
                            code = _gfal_rm_files(
                                files,
                                os.path.join(source_url, testing_folder, "src"))
                            if job_state != "FAILED":
                                # if job didn't faile remove dest files too
                                code = _gfal_rm_files(
                                    files,
                                    os.path.join(dest_url, testing_folder,
                                                 "dest"))


if __name__ == '__main__':
    main()
