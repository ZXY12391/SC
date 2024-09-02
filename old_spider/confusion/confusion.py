from urllib.parse import quote
from random import shuffle
import random

keyword_path = './words.txt'
F_confusion_info_path = './F_url.txt'
T_confusion_info_path = './T_confusion_info.txt'

__all__ = ['get_confusion_url']


def load_confusion_infos(c_type):
    """
    读取混淆的搜索
    :return:
    """
    if c_type == 'keywords':
        file = keyword_path
    elif c_type in ['T_profile', 'T_follower', 'T_following', 'T_post']:
        file = T_confusion_info_path
    elif c_type in ['F_profile', 'F_follower', 'F_following', 'F_post']:
        file = F_confusion_info_path
    else:
        return []

    with open(file, 'r', encoding='utf-8') as f:
        datas = f.readlines()
    return [i.strip() for i in datas if i.strip()]


def _confusion(target_list, rate=1.0, c_type='keywords'):
    """

    :param target_list: 采集任务
    :param rate:加入干扰混任务的数量，比例
    :return:混淆后的任务
    """
    confusions = load_confusion_infos(c_type)
    confusions_len = len(confusions)
    confusion_count = 1 + int(rate * len(target_list))
    if confusion_count >= confusions_len:
        target_list += confusions
    else:
        # random.sample(list1, count)：在list1中随机选取count个元素
        random_index = random.sample(range(0, confusions_len), confusion_count)
        for index in random_index:
            target_list.append(confusions[index])
    return target_list


def get_confusion_url(real_url, confusion_type='behavior'):
    """
    目标混淆：返回与real_url相同任务类型的混淆目标url（比如real_url采post，则需要返回一个采集post的混淆目标url）
    行为混淆：随机返回一个混淆目标url（不需要任务类型相同）
    :param real_url: 当前真正请求的
    :param confusion_type: 混淆类型，默认为行为混淆
    :return:
    """
    confusion_types = {'search': 'keywords', 'UserByScreenName': 'T_profile', 'Followers': 'T_follower',
                       'Following': 'T_following', 'timeline': 'T_post'}
    # 默认为行为混淆，即任务类型不同
    c_type = random.choice(list(confusion_types.values()))
    # 如果为目标混淆但是当前采集任务不为post、follower、following、profile则也会采用行为混淆
    if confusion_type == 'target':
        for k, v in confusion_types.items():
            if k in real_url:
                c_type = v
                break
    confusion_targets = _confusion(target_list=[real_url], rate=0.5, c_type=c_type)
    # 返回的目标列表包括了真实目标，这里需要将其删除
    confusion_targets.remove(real_url)
    # 如果爬取任务类型不为上述5种，即返回的confusion_targets为[]，则不进行混淆
    if not confusion_targets:
        return ''
    c_target = random.choice(confusion_targets)

    url = ''
    if c_type == 'keywords':
        url = 'https://twitter.com/i/api/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&q={}&count={}&query_source=typed_query&pc=0&spelling_corrections=1&ext=mediaStats%2ChighlightedLabel'.format(quote(c_target), 20)

    if c_type == 'T_profile':
        screen_name = c_target.split(',')[0][20:]
        url = 'https://api.twitter.com/graphql/-xfUfZsnR_zqjFd-IfrN5A/UserByScreenName?variables=%7B%22screen_name%22%3A%22{}%22%2C%22withHighlightedLabel%22%3Atrue%7D'.format(quote(screen_name))

    if c_type == 'T_follower':
        screen_id = c_target.split(',')[1]
        url = 'https://api.twitter.com/graphql/mA9n2AcGj94QffGv3wXV1Q/Followers?variables=%7B%22userId%22%3A%22{}%22%2C%22count%22%3A{}%2C%22withHighlightedLabel%22%3Afalse%2C%22withTweetQuoteCount%22%3Afalse%2C%22includePromotedContent%22%3Afalse%2C%22withTweetResult%22%3Afalse%2C%22withUserResult%22%3Afalse%7D'.format(quote(screen_id), 20)

    if c_type == 'T_following':
        screen_id = c_target.split(',')[1]
        url = 'https://api.twitter.com/graphql/oVUtpYtda5IGQ7sW8pE5VA/Following?variables=%7B%22userId%22%3A%22{}%22%2C%22count%22%3A{}%2C%22withHighlightedLabel%22%3Afalse%2C%22withTweetQuoteCount%22%3Afalse%2C%22includePromotedContent%22%3Afalse%2C%22withTweetResult%22%3Afalse%2C%22withUserResult%22%3Afalse%7D'.format(quote(screen_id), 20)

    if c_type == 'T_post':
        screen_id = c_target.split(',')[1]
        url = 'https://twitter.com/i/api/2/timeline/profile/{}.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&include_tweet_replies=true&count={}&userId={}&ext=mediaStats%2ChighlightedLabel'.format(screen_id, 20, screen_id)

    return url

if __name__=='__main__':
    confusion_types = {'search': 'keywords', 'UserByScreenName': 'T_profile', 'Followers': 'T_follower',
                       'Following': 'T_following', 'timeline': 'T_post'}
    print(random.sample(range(0, 7), 5))
#OK 了