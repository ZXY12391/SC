# coding:utf-8
import json
from typing import Dict
import re
# 对 Json 文件进行过滤


class FilterSecurity:
    @staticmethod
    def loadJson(path):
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data

    @staticmethod
    # 接收字符串判断
    def checkJsonSecurity(value):
        ##################################
        def filterSql(v):
            v = str(v)
            sqlPattern1 = r'select .*?where'
            sqlPattern2 = r'select .*?from'
            sqlPattern3 = r'insert.*?into .*?values'
            sqlPattern4 = r'update.*?='
            sqlPattern5 = r'delete.*?='
            sqlPatternList = [sqlPattern1, sqlPattern2, sqlPattern3, sqlPattern4, sqlPattern5]
            for p_i in sqlPatternList:
                results = re.findall(p_i, v, re.I)
                # print("p_i为: {}".format(p_i))
                if results:
                    # for r_i in results:
                    print("{} 存在疑似SQL注入.".format(v))
                        # v = v.replace(r_i, '')

            return v
        ###############################
        v_new = filterSql(value)
        return v_new

    @staticmethod
    # 递归遍历 Json，仅对 Key 值进行 安全校验，仅对存在危险的部分进行打印，未作处理
    def traverseJson(j_obj):
        if isinstance(j_obj, Dict):
            total = dict()
            for key, value in j_obj.items():
                total.update({key: FilterSecurity.traverseJson(value)})
            return total
        else:
            return FilterSecurity.checkJsonSecurity(j_obj)

    # 只做测试用
    @staticmethod
    def compare_two_json(j_1, j_2):
        if isinstance(j_1, Dict):
            for i in j_1:
                if i not in j_2:
                    print("j_2没有i {}".format(i))
                FilterSecurity.compare_two_json(j_1[i], j_2[i])
        else:
            if str(j_1) != str(j_2):
                print("j_1, j_2 不同: {}, {}".format(j_1, j_2))


if __name__ == '__main__':
    # 测试
    json_path = './json/tweet_json.json'
    # data = filterSecurity.loadJson(json_path)
    # print(len(data))
    data = {1: {2: 'hehe'}, 3: {4: "DELETE FROM Person WHERE LastName = 'Wilson'"}}
    data2 = FilterSecurity.traverseJson(data)
    # print(data2)
    # print(len(data2))
    # filterSecurity.compare_two_json(data, data2)
