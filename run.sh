#!/usr/bin/env bash

time cat ~/tdata/master.json.gz | zcat |\
    go run . \
    --workers 8 \
    --cell-level 23 \
    --batch-size 100000 \
    --cache-size 10000000 \
    --compression-level 9

# year=$(date +%Y)
# month=$(date +%m)
# year=2012
# last_year=$((year-1))
# time zgrep -ish -E '(Time"\:"'$year'-)' ~/tdata/master.json.gz # | tac | go run . --workers 8 --batch-size 10000 --duplicate-quit-limit 1000000
