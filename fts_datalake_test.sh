#!/bin/bash

export LOCALPATH="/tmp/ridona/temp_files_fts"

kdestroy # ad-hoc solution for account:ridona@eulake
/usr/bin/voms-proxy-init -voms escape
export X509_USER_PROXY=/tmp/x509up_u127450

mkdir -p $LOCALPATH

python fts_datalake_test.py -i conf/datalake.json --cleanup
