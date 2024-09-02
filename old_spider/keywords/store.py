import os
import pandas as pd
import pymongo

client = pymongo.MongoClient("mongodb://172.31.106.104:27017/")
db = client["DW"]
collection = db["tweets_user"]

# 指定目录路径
directory = '/data1/zxy/old_spider/data'

# 遍历目录下的所有文件
for filename in os.listdir(directory):
    
    if filename.startswith("filtered") and filename.endswith(".csv"):
        # 构建文件路径
        file_path = os.path.join(directory, filename)
        
        # 读取 CSV 文件
        df = pd.read_csv(file_path)
        
        # 将数据转换为字典格式
        data = df.to_dict(orient='records')
        
        # 插入数据到 MongoDB
        collection.insert_many(data)


# 关闭 MongoDB 连接
client.close()
