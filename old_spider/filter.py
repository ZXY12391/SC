import os
import pandas as pd

# 指定目录路径
directory = '/data1/zxy/old_spider/data'


# 从1.txt中读取关键字
txt_keywords_file = r'/data1/zxy/old_spider/keywords/tech.txt'
txt_keywords_path = os.path.join(directory, txt_keywords_file)
if os.path.isfile(txt_keywords_path):
    with open(txt_keywords_path, 'r') as f:
        txt_keywords = f.read().splitlines()
else:
    txt_keywords = []
# 遍历目录下的所有文件
for filename in os.listdir(directory):
    if filename.endswith(".csv"):
        # 构建文件路径
        file_path = os.path.join(directory, filename)
        
        # 读取 CSV 文件
        df = pd.read_csv(file_path)
        
        # 删除不是原创的行
        df = df[df['tweet_type'] == '原创']
        
        # 过滤包含特定字符串的内容
        keyword=['CVE', 'cyberattck', 'phishing','vulnerability','malware','password stolen','information stolen','zero-day','rootkits','botnet','trojans','adware','zero day','data leak', 'data breach', 'hacker', 'cyberattack', 'information leak','Ransomware','Identity Theft','Phishing','hacker attack', 'DDoS attack']
        keywords= txt_keywords+keyword

        filter_condition = df['content'].str.contains('|'.join(keywords), case=False)
        df = df[filter_condition]
        
        # 可选：如果你想要重设索引
        df.reset_index(drop=True, inplace=True)
        
        # 构建新的文件路径
        new_file_path = os.path.join(directory, "filtered_" + filename)
        
        # 保存到新的 CSV 文件
        df.to_csv(new_file_path, index=False)
