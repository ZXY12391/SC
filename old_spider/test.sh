#!/bin/bash
# 检查参数数量
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <keyword_list> <datestart> <num_months>"
    exit 1
fi
# 获取参数
KEYWORD_LIST=$1
DATESTART=$2
NUM_MONTHS=$3
# 运行 Python 脚本
python /data1/zxy/old_spider/keyword_crawl.py "$KEYWORD_LIST" "$DATESTART" "$NUM_MONTHS"
