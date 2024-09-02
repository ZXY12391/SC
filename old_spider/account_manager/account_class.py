class SpiderAccount:
    def __init__(self, account_name, **paras):
        self.account_name = account_name
        self.proxies = paras['proxies']
        self.cookies = paras['cookies']
        self.alive = 1
        self.crawled_url_info = []


