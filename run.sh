#!/usr/bin/env bash

time cat ~/tdata/master.json.gz | zcat |\
    go run . \
    --workers 8 \
    --cell-level 23 \
    --batch-size 100000 \
    --cache-size 50000000 \
    --compression-level 9
# =>
# ...
# 2023/12/15 11:40:15 main.go:414: cat=jr uniq=410950 uniq.perc=22.73 dup=1397252 total=1808202
# 2023/12/15 11:40:15 main.go:414: cat=_ uniq=116 uniq.perc=7.85 dup=1361 total=1477
# 2023/12/15 11:40:15 main.go:414: cat=iPhone uniq=1418438 uniq.perc=10.37 dup=12265866 total=13684304
# 2023/12/15 11:40:15 main.go:414: cat=rj uniq=800663 uniq.perc=36.27 dup=1406683 total=2207346
# 2023/12/15 11:40:15 main.go:414: cat=tester uniq=1 uniq.perc=9.09 dup=10 total=11
# 2023/12/15 11:40:15 main.go:414: cat=pr uniq=282184 uniq.perc=8.05 dup=3221057 total=3503241
# 2023/12/15 11:40:15 main.go:414: cat=kk uniq=251617 uniq.perc=15.75 dup=1346404 total=1598021
# 2023/12/15 11:40:15 main.go:414: cat=jlc uniq=167410 uniq.perc=22.05 dup=591806 total=759216
# 2023/12/15 11:40:15 main.go:414: cat=ric uniq=715 uniq.perc=89.04 dup=88 total=803
# 2023/12/15 11:40:15 main.go:414: cat=kd uniq=45318 uniq.perc=16.16 dup=235032 total=280350
# 2023/12/15 11:40:15 main.go:414: cat=mat uniq=393655 uniq.perc=4.68 dup=8020544 total=8414199
# 2023/12/15 11:40:15 main.go:414: cat=Chishiki uniq=807470 uniq.perc=7.56 dup=9872258 total=10679728
# 2023/12/15 11:40:15 main.go:414: cat=iPhone_8 uniq=3118 uniq.perc=41.62 dup=4373 total=7491
# 2023/12/15 11:40:15 main.go:414: cat=pancho uniq=794 uniq.perc=88.32 dup=105 total=899
# 2023/12/15 11:40:15 main.go:414: cat=rye uniq=10209543 uniq.perc=7.56 dup=124900254 total=135109797
# 2023/12/15 11:40:15 main.go:414: cat=ia uniq=9741005 uniq.perc=22.46 dup=33632830 total=43373835
# 2023/12/15 11:40:15 main.go:414: cat=QP1A.191005.007.A3_Pixel_XL uniq=143 uniq.perc=1.26 dup=11194 total=11337
#
# real    14m22.720s
# user    52m43.999s
# sys     1m33.932s



# year=$(date +%Y)
# month=$(date +%m)
# year=2012
# last_year=$((year-1))
# time zgrep -ish -E '(Time"\:"'$year'-)' ~/tdata/master.json.gz # | tac | go run . --workers 8 --batch-size 10000 --duplicate-quit-limit 1000000
