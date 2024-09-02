import hashlib
import redis

from config.settings import (REDIS_DB, REDIS_HOST, REDIS_PASS, REDIS_PORT,
                             BLOOM_FILTER_FOLLOWER, BLOOM_FILTER_POST, BLOOM_FILTER_FOLLOWING, BLOOM_FILTER_USER,
                             BLOOM_FILTER_KEYWORD_POST,BLOOM_FILTER_USER_POST
                             )


class MultipleHash(object):
    def __init__(self, salts, hash_func_name='md5'):
        """
        该类实现对某个数值进行加盐hash的过程
        :param salts: 对原始的数据进行预定义加盐
        :param hash_func_name: 可使用多个 hash 函数
        """
        self.hash_func = getattr(hashlib, hash_func_name)
        if len(salts) < 3:
            raise Exception("请至少输入 3 个 salts")
        self.salts = salts

    def _safe_data(self, data):
        """
        对即将hash的数据进行预处理
        这里我已经确认我运行在 py3 环境中
        就不像之前一样对系统进行判断
        :param data:
        :return:
        """
        if isinstance(data, str):
            return data.encode()
        elif isinstance(data, bytes):
            return data
        else:
            raise Exception("被hash值必须是一个字符串")

    def get_hash_value(self, data):
        """
        根据提供的数据 返回多个hash函数值
        :param data:
        :return:
        """
        hash_values = []
        for i in self.salts:
            hash_obj = self.hash_func()
            hash_obj.update(self._safe_data(data))
            hash_obj.update(self._safe_data(i))
            ret = hash_obj.hexdigest()
            # 将结果的 16 进制字节转换为 10 进制
            hash_values.append(int(ret, 16))
        return hash_values


class BloomFilter(object):
    def __init__(self,
                 redis_key,
                 memory_size,
                 redis_host=REDIS_HOST,
                 redis_port=REDIS_PORT,
                 redis_db=REDIS_DB,
                 salts=('f', 'e', 'n', 'g', 's', 'o', 'n', 'g'),
                 ):

        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_key = redis_key
        self.client = self.get_redis_client()
        self.multihash = MultipleHash(salts)
        self.memory_size = memory_size

    def get_redis_client(self):
        """
        获取一个redis连接对象
        :return:
        """
        pool = redis.ConnectionPool(host=self.redis_host, port=self.redis_port, db=self.redis_db)
        client = redis.StrictRedis(connection_pool=pool)
        return client

    def _get_offset(self, hash_value):
        return hash_value % (self.memory_size * 1024 * 1024 * 8)  # 使用128M内存

    def save(self, data):
        """
        将值存入布隆过滤器
        :param data:
        :return:
        """
        hash_values = self.multihash.get_hash_value(data)
        for hash_value in hash_values:
            offset = self._get_offset(hash_value)
            self.client.setbit(self.redis_key, offset, 1)
        return True

    def is_exist(self, data):
        """
        判断某个值在布隆过滤器中是否存在
        :param data:
        :return: 已存在则返回True，不存在则返回False
        """
        hash_values = self.multihash.get_hash_value(data)
        for hash_value in hash_values:
            offset = self._get_offset(hash_value)
            ret = self.client.getbit(self.redis_key, offset)
            if not ret:
                return False
        return True


# 布隆过滤器
bloom_filter_user = BloomFilter(redis_key=BLOOM_FILTER_USER, memory_size=128)  # 存储所有的目标用户任务url（T_user_task表中）
bloom_filter_post = BloomFilter(redis_key=BLOOM_FILTER_POST, memory_size=128)
bloom_filter_follower = BloomFilter(redis_key=BLOOM_FILTER_FOLLOWER, memory_size=128)
bloom_filter_following = BloomFilter(redis_key=BLOOM_FILTER_FOLLOWING, memory_size=128)
bloom_filter_keyword_post = BloomFilter(redis_key=BLOOM_FILTER_KEYWORD_POST, memory_size=128)
bloom_filter_user_post=BloomFilter(redis_key=BLOOM_FILTER_USER_POST, memory_size=128)


def insert_bloom_filter(data):
    if bloomfilter.is_exist(data):
        print("{} 已经存在于布隆过滤器中".format(data))
    else:
        bloomfilter.save(data)
        print("{} 存储布隆过滤器成功".format(data))


if __name__ == "__main__":
    # h = MultipleHash(salts=['1', '2', '3'])
    # print(h.get_hash_value("ruiyang"))
    datas = ['.浮三方大']
    bloomfilter = BloomFilter(redis_key='bloom_filter:user_url', memory_size=128)
    for data in datas:
        if bloomfilter.is_exist(data):
            print("{} 已经存在".format(data))
        else:
            bloomfilter.save(data)
            print("{} 存储成功".format(data))


# 基于 redis 的布隆过滤器的实现
# 布隆过滤器=》判断数据X不存在，则X必然不存在；而判断X存在，则X也可能不存在---存在误报
# （1） 多个 hash 函数的实现和求值
# （2） hash 表的实现以及实现对应的映射以及判断
#解决，在前端看不到，不知道为啥