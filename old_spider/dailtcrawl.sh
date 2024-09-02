#!/bin/bash

# 确保传入了参数
if [ "$#" -lt 2 ]; then
    echo "Usage: $0 MODE [DAYS|KEYWORDS] [KEYWORDS...] [DATESTART]"
    exit 1
fi

# 解析模式
MODE=$1
shift

# 根据模式处理参数
if [ "$MODE" == "days" ]; then
    if [ "$#" -ne 1 ]; then
        echo "For 'days' mode, provide the number of days."
        exit 1
    fi
    DAYS=$1
    python /data1/zxy/old_spider/dailycraler.py --mode days --days $DAYS

elif [ "$MODE" == "keywords" ]; then
    if [ "$#" -lt 2 ]; then
        echo "For 'keywords' mode, provide keywords and start date."
        exit 1
    fi
    DATESTART=${!#}
    unset 'KEYWORDS[${#KEYWORDS[@]}-1]'
    KEYWORDS=("$@")
    python /data1/zxy/old_spider/dailycraler.py --mode keywords --keywords "${KEYWORDS[@]}" --datestart $DATESTART

else
    echo "Unknown mode selected. Please choose 'days' or 'keywords'."
    exit 1
fi
