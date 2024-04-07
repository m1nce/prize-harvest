#!/bin/bash
#
# Pipe the output of generate_dates.sh into GNU Parallel, running players_scrape.sh for each date

./generate_dates.sh | parallel --delay 2 -j 1 ./players_scrape.sh {}