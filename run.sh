#!/usr/bin/env bash

time cat ~/tdata/master.json.gz | zcat | go run . --workers 8 --batch-size 10000
