import random
import time
import argparse
from keyword_new_simple.cookies_list import dx
from keyword_new_simple.get_keyword_tweets import get_keywords_tweet
from datetime import datetime as dt
import datetime

def get_cookie_dict(x):
    xx = x.split(';')
    c_d = {}
    for xxx in xx:
        z = xxx.strip()
        key = z.split('=')
        c_d[key[0]] = key[1]
    return c_d

def fanti(text):
    import zhconv
    return zhconv.convert(text, 'zh-tw')

def crawl_days(days):
    keywords = ['勒索攻击','身份盗窃','网络钓鱼','DDOS攻击','黑客攻击','数据泄露','信息泄露','网络攻击','零日攻击',
                '恶意软件','密码泄露','漏洞利用','安全漏洞','vulnerability','malware','password stolen',
                'information stolen','zero-day','rootkits','botnet','trojans','adware','zero day','data leak',
                'data breach','hacker','cyberattack','information leak','Ransomware','Identity Theft',
                'Phishing','hacker attack','DDoS attack','CVE','zero-day-exploit']
    
    datestart = dt.now().date()
    flag = datestart
    for key in keywords:
        t = datetime.timedelta(days=days)
        next_date = flag - t
        d2 = random.choice(dx)
        cookies = get_cookie_dict(d2['cookie'])
        proxies = d2['proxy']
        keyword = f'{key} until:{flag} since:{next_date}'
        try:
            tweet_list = get_keywords_tweet(cookies=cookies, keyword=keyword, proxies=proxies)
        except Exception as e:
            print(f"Error fetching tweets for keyword {key}: {e}")
        time.sleep(3)

def crawl_keyword(keyword_list, datestart):
    datestart = dt.strptime(datestart, '%Y-%m-%d')
    flag = datestart
    for key in keyword_list:
        for _ in range(12):
            t = datetime.timedelta(days=30)
            next_date = flag - t
            d2 = random.choice(dx)
            cookies = get_cookie_dict(d2['cookie'])
            proxies = d2['proxy']
            keyword = f'{key} until:{flag.date()} since:{next_date.date()}'
            try:
                tweet_list = get_keywords_tweet(cookies=cookies, keyword=keyword, proxies=proxies)
            except Exception as e:
                print(f"Error fetching tweets for keyword {key}: {e}")
            flag = next_date
            time.sleep(3)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Twitter keyword scraper")
    parser.add_argument('--mode', type=str, required=True, choices=['days', 'keywords'], 
                        help="Mode of operation: 'days' to scrape for a number of days, 'keywords' to scrape based on keywords list")
    parser.add_argument('--days', type=int, help="Number of days for 'days' mode")
    parser.add_argument('--keywords', nargs='+', help="List of keywords for 'keywords' mode")
    parser.add_argument('--datestart', type=str, help="Start date in YYYY-MM-DD format for 'keywords' mode")

    args = parser.parse_args()

    if args.mode == 'days' and args.days:
        crawl_days(args.days)
    elif args.mode == 'keywords' and args.keywords and args.datestart:
        crawl_keyword(args.keywords, args.datestart)
    else:
        print("Please provide the correct arguments for the selected mode.")
