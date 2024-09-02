# -*- coding: utf-8 -*-
# @Time    : 2021-10-19 17:00
# @Author  : lldzyshwjx


from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


def es_query(index, condition, size):
    query_body = {
        "query": {
            "bool": {
                "must": condition
            }
        }
    }
    es = Elasticsearch(['10.0.12.2:9200'], ignore=[400, 405, 502])  # 以列表的形式忽略多个状态码
    result = es.search(index=index, doc_type='doc', body=query_body, size=size)
    return [dict(r.get('_source'), **{"_id": r.get("_id")}) for r in result.get('hits').get('hits')]


def es_update(index, update_data_list):
    """
    批量更新
    :param index:
    :param update_data_list:
    :return:
    """
    actions = []
    for item in update_data_list:
        _id = item.pop("_id")
        index_action = {
            '_op_type': 'update',
            '_index': index,
            '_type': "doc",
            '_id': _id,
            'doc': item
        }
        actions.append(index_action)

    if actions:
        es = Elasticsearch(['10.0.12.2:9200'], ignore=[400, 405, 502])  # 以列表的形式忽略多个状态码
        success, failed = bulk(es, actions)
        print("共{}条数据,更新成功{}条,更新失败{}条".format(len(actions), success, len(failed)))
        if failed:
            print('更新失败数据为:', failed)


def es_delete(index, _id):
    es = Elasticsearch(['10.0.12.2:9200'], ignore=[400, 405, 502])  # 以列表的形式忽略多个状态码
    rs = es.delete(index, doc_type="doc", id=_id)


def statistic_common_friends():
    """
    2022-03-28 帮东哥统计https://twitter.com/VPNRanks和https://twitter.com/weareproprivacy的好友中，在确认vpn和不确认vpn中关键转发和起始发布的共同好友
    :return:
    """
    index = 't_user_relationship_3'
    condition = [
        {"term": {"user_url": "https://twitter.com/VPNRanks"}},
        # {"term": {"tourist_crawl_status": 1}},

        # {"term": {"is_useful": "1"}},
        # {"term": {"is_useful": "0"}},
        # {"match": {"author_url": {"query": "https://twitter.com/RinhoaSakura", "operator": "and"}}},
        # {"match": {"content": {"query": "vpn", "operator": "and"}}},
        # {"match_all": {}}

    ]
    size = 100000
    cmts = es_query(index, condition, size)
    vpn_ranks_friends = set([u.get('relationship_user_url') for u in cmts])

    condition = [
        {"term": {"user_url": "https://twitter.com/weareproprivacy"}},
        # {"term": {"tourist_crawl_status": 1}},

        # {"term": {"is_useful": "1"}},
        # {"term": {"is_useful": "0"}},
        # {"match": {"author_url": {"query": "https://twitter.com/RinhoaSakura", "operator": "and"}}},
        # {"match": {"content": {"query": "vpn", "operator": "and"}}},
        # {"match_all": {}}

    ]
    size = 100000
    cmts = es_query(index, condition, size)
    pro_privacy_friends = set([u.get('relationship_user_url') for u in cmts])
    print("vpn_ranks_friends:", len(vpn_ranks_friends), vpn_ranks_friends)
    print("pro_privacy_friends:", len(pro_privacy_friends), pro_privacy_friends)
    import pandas as pd
    real_vpn = pd.read_excel('../../vpn_count/count/count/data/确认_VPN详细信息.xlsx', index_col=False)
    uncertain_vpn = pd.read_excel('../../vpn_count/count/count/data/不确认_VNP详细信息.xlsx', index_col=False)
    vpns = pd.concat([real_vpn, uncertain_vpn], ignore_index=True)

    max_retweet = vpns[vpns['最大转发量'] != 0][['最大转发量发布者', '最大转发量发布者名字']]
    first_post = vpns[['起始发布者', '起始发布者名字']]

    max_retweet_url = set(max_retweet['最大转发量发布者'].to_list())
    first_post_url = set(first_post['起始发布者'].to_list())

    vpnranks_union_max_retweet = max_retweet_url & vpn_ranks_friends
    vpnranks_union_first_post = first_post_url & vpn_ranks_friends
    print("vpnranks_union_max_retweet:", len(vpnranks_union_max_retweet), vpnranks_union_max_retweet)
    print("vpnranks_union_first_post:", len(vpnranks_union_first_post), vpnranks_union_first_post)

    vpnranks_max_retweet = max_retweet[max_retweet['最大转发量发布者'].isin(vpnranks_union_max_retweet)]
    vpnranks_first_post = first_post[first_post['起始发布者'].isin(vpnranks_union_first_post)]
    vpnranks_max_retweet.drop_duplicates('最大转发量发布者', inplace=True)
    vpnranks_first_post.drop_duplicates('起始发布者', inplace=True)
    print("vpnranks_max_retweet")
    print(vpnranks_max_retweet)
    print("vpnranks_first_post")
    print(vpnranks_first_post)

    proprivacy_union_max_retweet = max_retweet_url & pro_privacy_friends
    proprivacy_union_first_post = first_post_url & pro_privacy_friends
    print("proprivacy_union_max_retweet:", len(proprivacy_union_max_retweet), proprivacy_union_max_retweet)
    print("proprivacy_union_first_post:", len(proprivacy_union_first_post), proprivacy_union_first_post)
    proprivacy_max_retweet = max_retweet[max_retweet['最大转发量发布者'].isin(proprivacy_union_max_retweet)]
    proprivacy_first_post = first_post[first_post['起始发布者'].isin(proprivacy_union_first_post)]
    proprivacy_max_retweet.drop_duplicates('最大转发量发布者', inplace=True)
    proprivacy_first_post.drop_duplicates('起始发布者', inplace=True)
    print("proprivacy_max_retweet")
    print(proprivacy_max_retweet)
    print("proprivacy_first_post")
    print(proprivacy_first_post)

    proprivacy_max_retweet['user_url'] = 'https://twitter.com/weareproprivacy'
    proprivacy_first_post['user_url'] = 'https://twitter.com/weareproprivacy'
    vpnranks_max_retweet['user_url'] = 'https://twitter.com/VPNRanks'
    vpnranks_first_post['user_url'] = 'https://twitter.com/VPNRanks'

    max_retweet_res = pd.concat([vpnranks_max_retweet, proprivacy_max_retweet])
    first_reweet_res = pd.concat([vpnranks_first_post, proprivacy_first_post])
    print(max_retweet_res)
    print(first_reweet_res)
    max_retweet_res.to_csv('../../vpn_count/count/count/data/关键传播者共同好友.csv', index=False, encoding='utf-8')
    first_reweet_res.to_csv('../../vpn_count/count/count/data/初始发布者共同好友.csv', index=False, encoding='utf-8')


# 获取强智杯恶意用户推文数据
def get_malicious_user_tweet():
    import pandas as pd
    user_urls = pd.read_csv('../qzb_data/crawl_qzb_malicious_user_profile.csv', index_col=False)['user_url'].to_list()
    tweets = []
    index = 't_user_post_3'
    for idx, url in enumerate(user_urls):
        condition = [
            {"term": {"author_url": url}},
        ]
        size = 5000
        res = es_query(index, condition, size)
        print(idx, len(res))
        # for r in res:
        #     print(r)
        tweets.extend(res)
    tweets = pd.DataFrame(tweets)
    print(tweets.shape)
    tweets.to_csv('../qzb_data/crawl_qzb_malicious_user_tweet.csv', index=False)


def get_malicious_user_relationship():
    import pandas as pd
    user_urls = pd.read_csv('../qzb_data/crawl_qzb_malicious_user_profile.csv', index_col=False)['user_url'].to_list()
    relationships = []
    index = 't_user_relationship_3'
    for idx, url in enumerate(user_urls):
        condition = [
            {"term": {"user_url": url}},
        ]
        size = 5000
        res = es_query(index, condition, size)
        print(idx, len(res))
        # for r in res:
        #     print(r)
        relationships.extend(res)
    relationships = pd.DataFrame(relationships)
    print(relationships.shape)
    relationships.to_csv('../qzb_data/crawl_qzb_malicious_user_relationship.csv', index=False)


def get_malicious_user_tourist():
    import pandas as pd
    user_urls = pd.read_csv('../qzb_data/crawl_qzb_malicious_user_profile.csv', index_col=False)['user_url'].to_list()
    relationships = []
    index = 't_user_tourist_3'
    for idx, url in enumerate(user_urls):
        condition = [
            {"term": {"author_url": url}},
        ]
        size = 100000
        res = es_query(index, condition, size)
        print(idx, len(res))
        # for r in res:
        #     print(r)
        relationships.extend(res)
    relationships = pd.DataFrame(relationships)
    print(relationships.shape)
    relationships.to_csv('../qzb_data/crawl_qzb_malicious_user_tourist.csv', index=False)


if __name__ == '__main__':
    # get_malicious_user_tweet()
    # get_malicious_user_relationship()
    get_malicious_user_tourist()
    assert ()
    index = 't_user_post_info_3'
    # index = ''
    condition = [
        {"term": {"user_url": "https://twitter.com/VPNRanks"}},
        # {"term": {"tourist_crawl_status": 1}},

        # {"term": {"is_useful": "1"}},
        # {"term": {"is_useful": "0"}},
        # {"match": {"author_url": {"query": "https://twitter.com/RinhoaSakura", "operator": "and"}}},
        # {"match": {"content": {"query": "vpnu", "operator": "or"}}},
        # {"match_all": {}}

    ]
    size = 100000
    res = es_query(index, condition, size)
    for r in res:
        print(r)
    # # print(cmts[0].get('tweet_img_binary'))
    # es = Elasticsearch(['10.0.12.2:9200'], ignore=[400, 405, 502])  # 以列表的形式忽略多个状态码
    # print(es.indices)
    # for index in es.indices.get('*'):
    #     print(index)
