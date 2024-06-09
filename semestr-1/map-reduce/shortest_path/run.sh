#!/bin/bash

export vertext_count=$(python3 find_vertex_count.py data.txt --cat-output 2> /dev/null | awk '{print $2}')
python3 find_shortest_way.py --cat-output 2> /dev/null --startnode $1 --total_count $vertext_count $2