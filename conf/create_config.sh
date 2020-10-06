#!/bin/bash

export LOCALPATH="/afs/cern.ch/user/r/ridona/escape/wp2-github/Utilities-and-Operations-Scripts/cric-info-tools"
python $LOCALPATH/export_endpoints_fts_test_config.py -i $LOCALPATH/disabled_rses.txt -o datalake.json


