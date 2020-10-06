#!/bin/bash

export LOCALPATH="/afs/cern.ch/user/r/ridona/escape/wp2-github/fts-analysis-datalake/temp_files_fts"

kdestroy # ad-hoc solution for account:ridona@eulake
/usr/bin/voms-proxy-init -voms escape
export X509_USER_PROXY=/tmp/x509up_u127450

mkdir -p $LOCALPATH

python /afs/cern.ch/user/r/ridona/escape/wp2-github/fts-analysis-datalake/fts_datalake_test.py -i conf/datalake.json

