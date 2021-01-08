#!/bin/bash

export FTS_LOCALPATH="/tmp/ridona/temp_files_fts"

kdestroy # ad-hoc solution for account:ridona@eulake
/usr/bin/voms-proxy-init -voms escape
export X509_USER_PROXY=/tmp/x509up_u127450

mkdir -p $FTS_LOCALPATH

fts_testing(){
    export GFAL2_TIMEOUT=300
    export XRD_CONNECTIONWINDOW=$GFAL2_TIMEOUT
    export XRD_REQUESTTIMEOUT=$GFAL2_TIMEOUT
    export XRD_STREAMTIMEOUT=$GFAL2_TIMEOUT
    export XRD_TIMEOUTRESOLUTION=$GFAL2_TIMEOUT
    # python fts_datalake_test.py -i conf/datalake_all_1mb.json --cleanup --exit
    python fts_datalake_test.py -i conf/datalake_all_1mb.json
    # python fts_datalake_test.py -i conf/datalake_all_except_lapp_webdav_1000mb.json
}

fts_testing