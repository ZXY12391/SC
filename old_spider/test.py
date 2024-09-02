import time
# from httpx._exceptions import
import httpx
import requests
based_header = {
        'origin': 'https://twitter.com',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
        'authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',
        # 'referer': 'https://twitter.com',
        # 'x-twitter-active-user': 'yes',
        # 'x-twitter-auth-type': 'OAuth2Session',
        # 'x-twitter-client-language':'en',
        # 'authority': 'api.twitter.com',
        # 'method': 'GET',
        # 'accept-language': 'zh-CN,zh;q=0.9',
        # 'sec-ch-ua-mobile': '?0',
        # 'sec-ch-ua-platform': '"Windows"',
        # 'sec-fetch-dest':'empty',
        # 'sec-fetch-mode': 'cors',
        # 'sec-fetch-site':'same-site',
}

# cookie={'_ga_BYKEBDM7DS': 'GS1.1.1668430218.1.0.1668430218.0.0.0',
#         'auth_token': 'b42066a4904fe2cd3a68e6c466110b286a27462c',
#         'des_opt_in':'Y',
#         '_gcl_au':'1.1.1679964424.1671706644',
#         'mbox':'PC#66ed333c25a041b5a7d08733b0d0be1d.35_0#1734951457|session#7668a117abb049ed9f0488ffb0e2da02#1671708517',
#         '_ga_34PHSZMC42':'GS1.1.1671706643.1.1.1671706869.0.0.0',
#         '_ga':'GA1.2.1361456031.1667122339',
#         'guest_id_ads':'v1%3A167783074997576169',
#         'guest_id_marketing':'v1%3A167783074997576169',
#         'guest_id':'v1%3A167783074997576169',
#         'kdt':'MfPbhUgSFd1xvSnljvBUlgFM2ok5Hxe6Uullcxju',
#         '_gid':'GA1.2.142976139.1678260681',
#         'external_referer':'padhuUp37zjgzgv1mFWxJ12Ozwit7owX|0|8e8t2xd8A2w%3D',
#         'personalization_id' :'"v1_gFmwWIVKTQiy3uHqT0Rn8Q=="',
#         'ct0':'4ae8a8724f6315ff33704e538d2857d1c3847d7d9f74a6129d4ebf9f77c570116cd4abb50fbcdc36e82ac64d70d887bcc52819fdc0406302370bc56803601e872b5ade59699b4c92e76d28840b51e12b'
#         }
# c='external_referer=padhuUp37zjgzgv1mFWxJ12Ozwit7owX|0|8e8t2xd8A2w%3D; personalization_id="v1_gFmwWIVKTQiy3uHqT0Rn8Q=="'
headers={**based_header}
#headers['x-csrf-token'] ='4ae8a8724f6315ff33704e538d2857d1c3847d7d9f74a6129d4ebf9f77c570116cd4abb50fbcdc36e82ac64d70d887bcc52819fdc0406302370bc56803601e872b5ade59699b4c92e76d28840b51e12b'
#headers['cookie']="_ga_BYKEBDM7DS=GS1.1.1668430218.1.0.1668430218.0.0.0; des_opt_in=Y; _gcl_au=1.1.1679964424.1671706644; mbox=PC#66ed333c25a041b5a7d08733b0d0be1d.35_0#1734951457|session#7668a117abb049ed9f0488ffb0e2da02#1671708517; _ga_34PHSZMC42=GS1.1.1671706643.1.1.1671706869.0.0.0; _ga=GA1.2.1361456031.1667122339; guest_id_ads=v1:167783074997576169; guest_id_marketing=v1:167783074997576169; guest_id=v1:167783074997576169; kdt=MfPbhUgSFd1xvSnljvBUlgFM2ok5Hxe6Uullcxju; auth_token=b42066a4904fe2cd3a68e6c466110b286a27462c; ct0=4ae8a8724f6315ff33704e538d2857d1c3847d7d9f74a6129d4ebf9f77c570116cd4abb50fbcdc36e82ac64d70d887bcc52819fdc0406302370bc56803601e872b5ade59699b4c92e76d28840b51e12b; twid=u=1417123113267986438; _gid=GA1.2.142976139.1678260681; external_referer=padhuUp37zjgzgv1mFWxJ12Ozwit7owX|0|8e8t2xd8A2w=; personalization_id='v1_Ava0RIkvuyy5CXo/A7B2Zw=='"
#url='https://api.twitter.com/2/notifications/all.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&include_ext_has_nft_avatar=1&include_ext_is_blue_verified=1&include_ext_verified_type=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_ext_limited_action_results=false&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_ext_views=true&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&include_ext_sensitive_media_warning=true&include_ext_trusted_friends_metadata=true&send_error_codes=true&simple_quoted_tweet=true&count=20&requestContext=launch&ext=mediaStats%2ChighlightedLabel%2ChasNftAvatar%2CvoiceInfo%2CbirdwatchPivot%2Cenrichments%2CsuperFollowMetadata%2CunmentionInfo%2CeditControl%2Cvibe'
#'https://api.twitter.com/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&include_ext_has_nft_avatar=1&include_ext_is_blue_verified=1&include_ext_verified_type=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_ext_limited_action_results=false&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_ext_views=true&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&include_ext_sensitive_media_warning=true&include_ext_trusted_friends_metadata=true&send_error_codes=true&simple_quoted_tweet=true&q=台湾&result_filter=user&query_source=typed_query&count=20&requestContext=launch&pc=1&spelling_corrections=1&include_ext_edit_control=true&ext=mediaStats,highlightedLabel,hasNftAvatar,voiceInfo,birdwatchPivot,enrichments,superFollowMetadata,unmentionInfo,editControl,vibe'
#url='https://api.twitter.com/2/guide.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&include_ext_has_nft_avatar=1&include_ext_is_blue_verified=1&include_ext_verified_type=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_ext_limited_action_results=false&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_ext_views=true&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&include_ext_sensitive_media_warning=true&include_ext_trusted_friends_metadata=true&send_error_codes=true&simple_quoted_tweet=true&count=20&requestContext=launch&display_location=web_sidebar&include_page_configuration=false&entity_tokens=false&ext=mediaStats%2ChighlightedLabel%2ChasNftAvatar%2CvoiceInfo%2CbirdwatchPivot%2Cenrichments%2CsuperFollowMetadata%2CunmentionInfo%2CeditControl%2Cvibe'
# res=requests.get(url=url,headers=headers,cookies=cookie,timeout=10,proxies={'http': 'http://10.0.12.1:10802', 'https': 'http://10.0.12.1:10802'})
# print(res)
# print(res.text)
#url = "https://twitter.com/i/api/1.1/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&q={}&count=20&query_source=typed_query&pc=1&spelling_corrections=1&ext=mediaStats%2ChighlightedLabel%2CvoiceInfo"
# q = '(from:twitter) until:2021-01-02 since:2021-01-01'
# url_='https://api.twitter.com/2/search/adaptive.json?'
# params='include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&include_ext_has_nft_avatar=1&include_ext_is_blue_verified=1&include_ext_verified_type=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_ext_limited_action_results=false&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_ext_views=true&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&include_ext_sensitive_media_warning=true&include_ext_trusted_friends_metadata=true&send_error_codes=true&simple_quoted_tweet=true&q=taiwan%20until%3A2023-01-01%20since%3A2022-01-01&result_filter=user&query_source=typed_query&count=20&requestContext=launch&pc=1&spelling_corrections=1&include_ext_edit_control=true&ext=mediaStats%2ChighlightedLabel%2ChasNftAvatar%2CvoiceInfo%2CbirdwatchPivot%2Cenrichments%2CsuperFollowMetadata%2CunmentionInfo%2CeditControl%2Cvibe'
#
# #url='https://api.twitter.com/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&include_ext_has_nft_avatar=1&include_ext_is_blue_verified=1&include_ext_verified_type=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_ext_limited_action_results=false&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_ext_views=true&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&include_ext_sensitive_media_warning=true&include_ext_trusted_friends_metadata=true&send_error_codes=true&simple_quoted_tweet=true&q=taiwan%20until%3A2023-01-01%20since%3A2022-01-01&result_filter=user&count=20&query_source=typed_query&cursor=DAAFCgABFqw0c4Q__z8LAAIAAADwRW1QQzZ3QUFBZlEvZ0dKTjB2R3AvQUFBQUJRTmp2dmR5VlJBQWhCdFUvWFlsQkFCQzErRkhaYldJQUFTTGxLdWc1VFFBQTQxSFdja1ZQQUFBQUFBQVBISGlya0xRQ0Y5WHRjUUFBQUFBQURyMHl0c0FBQUFBQWZBS1BZQUFBQUFBNXRoYWdBQUFBREZNZS9pRVU1TG4rNVVFQUlTbDgycTNWVEFBQUFBQUFBQ2JzTklFTm5HK2t3VlVBQVIvMUNBVUZmUUFRQUFBQUMxdXYwTUVZbDlIcTVVWUFBQUFBQUFCenY3dlExWDZOZVdWYUFBAAA&pc=1&spelling_corrections=1&include_ext_edit_control=true&ext=mediaStats%2ChighlightedLabel%2ChasNftAvatar%2CvoiceInfo%2CbirdwatchPivot%2Cenrichments%2CsuperFollowMetadata%2CunmentionInfo%2CeditControl%2Cvibe'
# #'https://api.twitter.com/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&include_ext_has_nft_avatar=1&include_ext_is_blue_verified=1&include_ext_verified_type=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_ext_limited_action_results=false&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_ext_views=true&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&include_ext_sensitive_media_warning=true&include_ext_trusted_friends_metadata=true&send_error_codes=true&simple_quoted_tweet=true&q=%E5%8F%B0%E6%B9%BE&result_filter=user&query_source=typed_query&count=20&requestContext=launch&pc=1&spelling_corrections=1&include_ext_edit_control=true&ext=mediaStats%2ChighlightedLabel%2ChasNftAvatar%2CvoiceInfo%2CbirdwatchPivot%2Cenrichments%2CsuperFollowMetadata%2CunmentionInfo%2CeditControl%2Cvibe'
# cx='at_check=true; _gid=GA1.2.490664651.1678350038; mbox=PC#5ede305b874b4a9ab92022b4db0a4508.35_0#1741595283|session#4f4794e5d82d467ba7dfa43d40a2ebf7#1678352343; _ga_34PHSZMC42=GS1.1.1678350046.3.1.1678350494.0.0.0; _ga=GA1.2.1945272692.1669812117; guest_id_ads=v1%3A167835051223885130; guest_id_marketing=v1%3A167835051223885130; guest_id=v1%3A167835051223885130; gt=1633755252238295042; personalization_id="v1_ssaiboOTx3r5N2LhFaJOLw=="; ct0=3e0e26f5b6bd542b785eb99ee3068c02'
# #'_ga_BYKEBDM7DS=GS1.1.1668430218.1.0.1668430218.0.0.0; des_opt_in=Y; _gcl_au=1.1.1679964424.1671706644; mbox=PC#66ed333c25a041b5a7d08733b0d0be1d.35_0#1734951457|session#7668a117abb049ed9f0488ffb0e2da02#1671708517; _ga_34PHSZMC42=GS1.1.1671706643.1.1.1671706869.0.0.0; _ga=GA1.2.1361456031.1667122339; guest_id_ads=v1:167783074997576169; guest_id_marketing=v1:167783074997576169; guest_id=v1:167783074997576169; kdt=MfPbhUgSFd1xvSnljvBUlgFM2ok5Hxe6Uullcxju; auth_token=b42066a4904fe2cd3a68e6c466110b286a27462c; ct0=4ae8a8724f6315ff33704e538d2857d1c3847d7d9f74a6129d4ebf9f77c570116cd4abb50fbcdc36e82ac64d70d887bcc52819fdc0406302370bc56803601e872b5ade59699b4c92e76d28840b51e12b; twid=u=1417123113267986438; _gid=GA1.2.142976139.1678260681; external_referer=padhuUp37zjgzgv1mFWxJ12Ozwit7owX|0|8e8t2xd8A2w=; personalization_id="v1_cPA0e8/hIo1h47KOfUgbMw=="'
# cookie_str="_ga_BYKEBDM7DS=GS1.1.1668430218.1.0.1668430218.0.0.0; des_opt_in=Y; _gcl_au=1.1.1679964424.1671706644; mbox=PC#66ed333c25a041b5a7d08733b0d0be1d.35_0#1734951457|session#7668a117abb049ed9f0488ffb0e2da02#1671708517; _ga_34PHSZMC42=GS1.1.1671706643.1.1.1671706869.0.0.0; _ga=GA1.2.1361456031.1667122339; guest_id_ads=v1:167783074997576169; guest_id_marketing=v1:167783074997576169; guest_id=v1:167783074997576169; kdt=MfPbhUgSFd1xvSnljvBUlgFM2ok5Hxe6Uullcxju; auth_token=b42066a4904fe2cd3a68e6c466110b286a27462c; ct0=4ae8a8724f6315ff33704e538d2857d1c3847d7d9f74a6129d4ebf9f77c570116cd4abb50fbcdc36e82ac64d70d887bcc52819fdc0406302370bc56803601e872b5ade59699b4c92e76d28840b51e12b; twid=u=1417123113267986438; _gid=GA1.2.142976139.1678260681; external_referer=padhuUp37zjgzgv1mFWxJ12Ozwit7owX|0|8e8t2xd8A2w=; personalization_id='v1_Ava0RIkvuyy5CXo/A7B2Zw=='"
# cookie={}
# cc=cookie_str.split(';')
# for ccc in cc:
#         key_value=ccc.split('=')
#         key=key_value[0]
#         print(key)
#         value=key_value[1]
#         cookie[key.strip()]=value.strip()
# pp=params.split('&')
# p={}
# for ppp in pp:
#         key_value = ppp.split('=')
#         key = key_value[0]
#         value = key_value[1]
#         p[key.strip()] = value.strip()
#         print(key,p[key])
# print(p)
# from urllib import parse
# url_f='https://api.twitter.com/1.1/jot/client_event.json?keepalive=false'
# r=requests.post(url=url_f,headers=headers,proxies={'http': 'http://10.0.12.1:10802', 'https': 'http://10.0.12.1:10802'})
# time.sleep(0.5)
# print(r)
# # requests.options()params=p,

# headers['x-csrf-token'] =cookie['ct0']
# headers['x-guest-token']='1633755252238295042'#4ae8a8724f6315ff33704e538d2857d1c3847d7d9f74a6129d4ebf9f77c570116cd4abb50fbcdc36e82ac64d70d887bcc52819fdc0406302370bc56803601e872b5ade59699b4c92e76d28840b51e12b'



# url_post='https://api.twitter.com/1.1/jot/client_event.json?keepalive=false'
# res_=requests.get(url=url_post,headers=headers,cookies=cookie,timeout=10,proxies={'http': 'http://10.0.12.1:10802', 'https': 'http://10.0.12.1:10802'})
# time.sleep(0.2)
# url1='https://api.twitter.com/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&include_ext_has_nft_avatar=1&include_ext_is_blue_verified=1&include_ext_verified_type=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_ext_limited_action_results=false&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_ext_views=true&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&include_ext_sensitive_media_warning=true&include_ext_trusted_friends_metadata=true&send_error_codes=true&simple_quoted_tweet=true&q=taiwan%20until%3A2023-01-01%20since%3A2022-01-01&query_source=typed_query&count=20&requestContext=launch&pc=1&spelling_corrections=1&include_ext_edit_control=true&ext=mediaStats%2ChighlightedLabel%2ChasNftAvatar%2CvoiceInfo%2CbirdwatchPivot%2Cenrichments%2CsuperFollowMetadata%2CunmentionInfo%2CeditControl%2Cvibe'
# #'https://twitter.com/i/api/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&q={}&count={}&query_source=typed_query&pc=0&spelling_corrections=1&ext=mediaStats%2ChighlightedLabel'.format(parse.quote('台湾'), 20)
# res1=requests.options(url=url,headers=headers,cookies=cookie,timeout=10,proxies={'http': 'http://10.0.12.1:10802', 'https': 'http://10.0.12.1:10802'})
# time.sleep(0.2)
# u='https://api.twitter.com/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&include_ext_has_nft_avatar=1&include_ext_is_blue_verified=1&include_ext_verified_type=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_ext_limited_action_results=false&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_ext_views=true&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&include_ext_sensitive_media_warning=true&include_ext_trusted_friends_metadata=true&send_error_codes=true&simple_quoted_tweet=true&q=taiwan%20until%3A2023-01-01%20since%3A2022-01-01&result_filter=user&query_source=typed_query&count=20&requestContext=launch&pc=1&spelling_corrections=1&include_ext_edit_control=true&ext=mediaStats%2ChighlightedLabel%2ChasNftAvatar%2CvoiceInfo%2CbirdwatchPivot%2Cenrichments%2CsuperFollowMetadata%2CunmentionInfo%2CeditControl%2Cvibe'
# c='https://api.twitter.com/2/notifications/all.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&include_ext_has_nft_avatar=1&include_ext_is_blue_verified=1&include_ext_verified_type=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_ext_limited_action_results=false&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_ext_views=true&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&include_ext_sensitive_media_warning=true&include_ext_trusted_friends_metadata=true&send_error_codes=true&simple_quoted_tweet=true&count=40&cursor=DAABDAABCgABE6qh9plVgAYIAAIAAAABCAADSQ3bEQgABMVJWjAACwACAAAAC0FZYW1pams0YlV3AAA&ext=mediaStats%2ChighlightedLabel%2ChasNftAvatar%2CvoiceInfo%2CbirdwatchPivot%2Cenrichments%2CsuperFollowMetadata%2CunmentionInfo%2CeditControl%2Cvibe'
# uu='https://api.twitter.com/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&include_ext_has_nft_avatar=1&include_ext_is_blue_verified=1&include_ext_verified_type=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_ext_limited_action_results=false&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_ext_views=true&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&include_ext_sensitive_media_warning=true&include_ext_trusted_friends_metadata=true&send_error_codes=true&simple_quoted_tweet=true&q=%E5%8F%B0%E6%B9%BE%20until%3A2021-02-01%20since%3A2021-01-01&result_filter=user&query_source=typed_query&count=20&requestContext=launch&pc=1&spelling_corrections=1&include_ext_edit_control=true&ext=mediaStats%2ChighlightedLabel%2ChasNftAvatar%2CvoiceInfo%2CbirdwatchPivot%2Cenrichments%2CsuperFollowMetadata%2CunmentionInfo%2CeditControl%2Cvibe'
# # res=requests.get(url=u,headers=headers,cookies=cookie,timeout=10,proxies={'http': 'http://10.0.12.1:10802', 'https': 'http://10.0.12.1:10802'})
# # print(res)
# # print(res.text)
# # time.sleep(0.2)
# print(parse.quote('taiwan until:2021-03-06 since:2021-01-04'))
# q = 'taiwan until:2021-01-02 since:2021-01-01'# until:2021-01-02 since:2021-01-01
# u3='https://twitter.com/i/api/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&q={}&result_filter=user&count={}&query_source=typed_query&pc=1&spelling_corrections=1&ext=mediaStats%2ChighlightedLabel'.format(parse.quote(q), 200)
from urllib import parse

#no cookie test
url_token = 'https://api.twitter.com/1.1/guest/activate.json'
url = "https://twitter.com/i/api/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&q={}&count=20&query_source=typed_query&pc=1&spelling_corrections=1&ext=mediaStats%2ChighlightedLabel%2CvoiceInfo"

u_t='https://api.twitter.com/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&include_ext_has_nft_avatar=1&include_ext_is_blue_verified=1&include_ext_verified_type=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_ext_limited_action_results=false&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_ext_views=true&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&include_ext_sensitive_media_warning=true&include_ext_trusted_friends_metadata=true&send_error_codes=true&simple_quoted_tweet=true&q=taiwan%20until%3A2021-03-06%20since%3A2021-01-04&result_filter=user&query_source=typed_query&count=20&requestContext=launch&pc=1&spelling_corrections=1&include_ext_edit_control=true&ext=mediaStats%2ChighlightedLabel%2ChasNftAvatar%2CvoiceInfo%2CbirdwatchPivot%2Cenrichments%2CsuperFollowMetadata%2CunmentionInfo%2CeditControl%2Cvibe'
import jsonpath
import json
token = json.loads(httpx.post(url_token, headers=headers,proxies='http://10.0.12.1:10804').text)['guest_token']
headers['x-guest-token'] = token
print(token)
t='https://twitter.com/i/api/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&q=taiwan&result_filter=user&count=200&query_source=typed_query&pc=1&spelling_corrections=1&ext=mediaStats%2ChighlightedLabel'
first_url = 'https://twitter.com/i/api/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&q={}&tweet_search_mode=live&count={}&query_source=typed_query&pc=1&spelling_corrections=1&ext=mediaStats%2ChighlightedLabel'.format(
        parse.quote('taiwan'), 200)

time.sleep(0.1)

# print(res)
# print(res.text)
# content = json.loads(res.content)
# print(content)
# cursor = jsonpath.jsonpath(content, '$..cursor[value]')[1]
# users = jsonpath.jsonpath(content, '$..users[*]')
# print(users[-1]['screen_name'])
# print(users[-2]['screen_name'])
# print(users[-3]['screen_name'])
# for u in users:
#         print(u['screen_name'])

def download_with_cookies_keyword(url, cookies, proxies, confusion=True):
    import httpx
    based_header1 = {
            'origin': 'https://twitter.com',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.92 Safari/537.36',
            'authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',
    }
    based_header2 = {
            'origin': 'https://twitter.com',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
            'authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',
    }
    for x in list(based_header1.keys()):
            print(based_header1[x]==based_header2[x])
    headers = {**based_header2}
    # proxies=proxies['http']
    print(type(proxies))
    print(url)
    try:
        url_token = 'https://api.twitter.com/1.1/guest/activate.json'
        token = json.loads(httpx.post(url_token, headers=headers, proxies=proxies).text)['guest_token']
        print(token)
        headers['x-guest-token'] = token
        resp = httpx.get(url, proxies=proxies, headers=headers, timeout=10)#, cookies=cookies
        print(resp.content)
        return resp
    except :
        pass
    finally:
        import random
        time.sleep(random.randint(10, 15))
    return None
download_with_cookies_keyword(url=t,proxies='http://10.0.12.1:10804',cookies=None)
# next_url = 'https://twitter.com/i/api/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&q={}&result_filter=user&count={}&query_source=typed_query&cursor={}&pc=0&spelling_corrections=1&ext=mediaStats%2ChighlightedLabel'.format(
#         parse.quote(q), 20, parse.quote(cursor))
# res_=httpx.get(url=next_url,headers=headers,timeout=10,proxies='http://10.0.12.1:10802')#cookies=cookie,
# # print(res.text)
# content_ = json.loads(res_.content)
# cursor_ = jsonpath.jsonpath(content_, '$..cursor[value]')[1]
# users_ = jsonpath.jsonpath(content_, '$..users[*]')
# print(users_[0]['screen_name'])
# print(users_[1]['screen_name'])
# print(users_[2]['screen_name'])

# import zhconv
#
# # 簡体字の例文
# kan_text = '承蒙关照了'
# # 繁体字の例文
# han_text = '承蒙关照了,asdasda,xasxsaTT**H(JJPPAKXAXLKXKLAMXAXXMLXmxmAMX()))('
# han_text_ = zhconv.convert(han_text,'zh-hans')
# print(han_text)
# if han_text_==han_text:
#         print('等于')
#
# if han_text==kan_text:
#         print('等于')
#
# print('簡体字: ', zhconv.issimp(kan_text))
# # True
# print('繁体字: ', zhconv.issimp(kan_text))
#
# def is_fanti_sentence(s):
#         s_=zhconv.convert(s,'zh-hans')
#         if s_==s:
#                 return False
#         else:
#                 return True
#
# a='承蒙关照了'
# print(is_fanti_sentence(a))














