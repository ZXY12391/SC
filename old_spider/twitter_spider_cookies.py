import os
import time
import random
import json
import requests
import pymysql
import logging
import warnings
import asyncio
from pyppeteer import launch
from selenium import webdriver
from db.mongo_db import MongoSpiderAccountOper
from copy import copy


"""
账号进行cookies获取
"""

MODULE_PATH = os.path.dirname(__file__)

ch_options = webdriver.ChromeOptions()
# 此步骤很重要，设置为开发者模式，防止被各大网站识别出来使用了Selenium
ch_options.add_experimental_option('excludeSwitches', ['enable-automation'])
ch_options.add_experimental_option('useAutomationExtension', False)
ch_options.add_argument("--lang=en-US")
ch_options.add_argument('--start-maximized')
ch_options.add_argument('--disable-infobars')  # 禁用浏览器正在被自动化程序控制的提示
ch_options.add_argument('--incognito')

first_num = random.randint(55, 76)
third_num = random.randint(0, 3800)
fourth_num = random.randint(0, 140)


class FakeChromeUA:
    os_type = [
        '(Windows NT 6.1; WOW64)', '(Windows NT 10.0; WOW64)', '(X11; Linux x86_64)',
        '(Macintosh; Intel Mac OS X 10_12_6)'
    ]

    chrome_version = 'Chrome/{}.0.{}.{}'.format(first_num, third_num, fourth_num)

    @classmethod
    def get_ua(cls):
        return ' '.join(['Mozilla/5.0', random.choice(cls.os_type), 'AppleWebKit/537.36',
                         '(KHTML, like Gecko)', cls.chrome_version, 'Safari/537.36']
                        )

kwargs = {
    # 启用浏览器界面
    'headless': False,
    # 多开页面，解决卡死
    'dumpio': True,
    # 设置浏览器全屏
    'args': ['--start-maximized',
             # 取消沙盒模式，沙盒模式下权限太小
             '--no-sandbox',
             # # 设置浏览器界面大小
             '--window-size=1920,1080',
             # 关闭受控制提示：比如，Chrome正在受到自动测试软件的控制...
             '--disable-infobars',
             # # 允许跨域
             # '--disable-web-security',
             # # 使用代理
             # '--proxy-server=127.0.0.1:80',
             # # 不走代理的链接
             # '--proxy-bypass-list=*',
             # # 忽略证书错误
             # '--ignore-certificate-errors',
             # # # log 等级设置，如果出现一大堆warning，可以不使用默认的日志等级
             # '--log-level=5',
             ],
    'userDataDir': MODULE_PATH + '/Temporary',
    # 用户数据保存目录，这个最好指定一个，
    # 如果不指定，Chrome会自动创建一个临时目录使用，在退出浏览器时自动删除，
    # 在删除的时候可能会删除失败(不知道为什么出现权限问题，我用Windows)导致浏览器退出失败
    # 删除失败时出现报错：OSError: Unable to remove Temporary User Data
    # 或者Chrome进程没有退出，cpu狂飙到99%
}


def generate_random_time():
    return random.randint(5, 10)


def input_time_random():
    return random.randint(80, 120)


def StorageCookie(user, cookies):

    print('更新{}用户Cookies信息中...'.format(user['account']))
    MongoSpiderAccountOper.insert_or_update_one_data('account_info_for_spider', {'account': user.get('account')}, {'token': cookies, 'alive': 1, 'user_agent': user.get('user_agent'), 'update_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())})
    print('更新{}用户Cookies信息成功!'.format(user['account']))


# 可用
async def login_pyppeteer(user):
    user['user_agent'] = user['user_agent'] if user.get('user_agent') else FakeChromeUA.get_ua()
    # 添加浏览器头
    kwargs['args'].append('--user-agent={}'.format(user['user_agent']))
    # # 设置代理
    kwargs['args'].append('--proxy-server=10.0.12.1:{}'.format(user.get('proxies').get('http').split(':')[-1]))
    browser = await launch(kwargs)
    # 打开一个无痕浏览器
    context = await browser.createIncognitoBrowserContext()
    page = await context.newPage()
    await page.evaluateOnNewDocument(
        '''() =>{ Object.defineProperties(navigator,{ webdriver:{ get: () => false } }) }''')
    await page.setViewport(viewport={'width': 1920, 'height': 1080})
    # 是否启用JS，enabled设为False，则无渲染效果
    await page.setJavaScriptEnabled(enabled=True)
    # 设置浏览器语言为英语
    await page.setExtraHTTPHeaders({'Accept-Language': 'en'})

    try:
        # await page.goto('https://twitter.com/login')  # Get the content of https://twitter.com/
        await page.goto('https://twitter.com/i/flow/login')  # Get the content of https://twitter.com/

        await asyncio.sleep(random.randint(15, 20))  # This line of code WHEREVER wait for n [5-8] seconds.
    except Exception as e:
        print('访问登录页面异常:{}, 代理有问题'.format(e.args))
        MongoSpiderAccountOper.update_spider_account_status(user.get('account'), {'alive': 4})
        await context.close()
        await browser.close()
        return False, 'network error!'
    try:
        # 尽量在执行完一个操作之后就sleep一下，不然容易报错找不到input[name=username]类似的元素
        # page.type为在文本框中输入数据
        await page.type('input[name=username]', user.get('account'), {'delay': input_time_random()})  # 输入数字之间的间隔单位毫秒
        # await page.type("input[name=session\[username_or_email\]]", user.uid, {'delay': input_time_random()})  # 输入数字之间的间隔单位毫秒
        await asyncio.sleep(random.randint(1, 3))
        # 回车，进入输密码界面
        await page.keyboard.press('Enter')
        await asyncio.sleep(random.randint(1, 3))
        # a = (await page.xpath('//*[@id="layers"]/div[2]/div/div/div/div/div/div[2]/div[2]/div/div/div[2]/div[2]/div[2]/div'))[0]
        # await a.click()
        # await asyncio.sleep(random.randint(15, 20))
        await page.type('input[name=password]', user.get('password'), {'delay': input_time_random()})
        # await page.type('input[name=session\[password\]]', user.password, {'delay': input_time_random()})
        await asyncio.sleep(random.randint(1, 4))
        # 回车，登录
        await page.keyboard.press('Enter')
        # lgb = (await page.xpath('//*[@id="react-root"]/div/div/div[2]/main/div/div/div[2]/form/div/div[3]/div/div/span/span'))[0]
        # await lgb.click()
        await asyncio.sleep(random.randint(5, 10))
        # 判断处理登录需要验证的情况
        try:
            cur_text = await page.content()
            challenge = await page.J('#challenge_response')  # 找不到时返回：None
            # 如果需要验证
            if challenge:
                phone = user.get('phone_number')
                phone = phone[2:] if phone.startswith('44') or phone.startswith('63') else phone
                phone = phone.replace('+44', '').replace('+63', '')
                if 'Verify your identity by entering the phone number associated with your Twitter account.' in cur_text:
                    await page.type('#challenge_response', phone, {'delay': input_time_random()})
                if 'Verify your identity by entering the email address associated with your Twitter account.' in cur_text:
                    await page.type('#challenge_response', user.get('account'), {'delay': input_time_random()})
                if 'Verify your identity by entering the username associated with your Twitter account.' in cur_text:
                    await page.type('#challenge_response', user.get('account'), {'delay': input_time_random()})
                if "we've sent a confirmation code to" in cur_text:
                    print('需要手机或者邮箱验证码，账号无了!!')
                    return False, 'need auth code'
                await asyncio.sleep(generate_random_time())
                await page.click('#email_challenge_submit')
                await asyncio.sleep(random.randint(15, 20))
        except Exception as ex:
            print('验证页面发生异常：', ex.args)
            pass

        cur_text = await page.content()
        # 这里还需要处理手机验证错误的情况---还未验证
        if 'Incorrect. Please try again.' in cur_text:
            print('手机号、邮箱或者用户id验证错误')
            await context.close()
            await browser.close()
            return False, 'phone email uid verify error'
        # if '邮箱验证' in cur_text:

        # 处理人机验证的情况--已验证，没问题
        man_machine_auth1 = "We've temporarily limited some of your account features"
        man_machine_auth2 = "Are you a robot"
        man_machine_auth3 = "Pass a Google reCAPTCHA challenge"
        if man_machine_auth1 in cur_text or man_machine_auth2 in cur_text or man_machine_auth3 in cur_text:
            print('需要人机验证，{}账户登录失败!'.format(user.get('account')))
            time.sleep(generate_random_time())
            await context.close()
            await browser.close()
            return False, 'man machine auth'

        # 如何判断登陆成功？？---没有出现上述情况就表示登录成功
        StorageCookie(user, await page.cookies())
        await asyncio.sleep(generate_random_time())

        # 获取session
        session = requests.Session()
        for cookie in await page.cookies():
            session.cookies.set(cookie['name'], cookie['value'])
        # # # 设置代理
        session.proxies.update(user.get('proxies'))
        print('{}账户登录成功!'.format(user.get('account')))
        print('使用pyppeteer登录,故休眠10-30秒后再关闭浏览器...')
        await asyncio.sleep(random.randint(10, 30))
        print('浏览器已关闭...')
        await context.close()
        await browser.close()
        return True, session

    except Exception as ex:
        print('发生异常{}'.format(ex.args))
        await context.close()
        await browser.close()
        return False, 'exception login failed!'


def login(user):
    # return login_selenium(user)
    return asyncio.get_event_loop().run_until_complete(login_pyppeteer(user))


if __name__ == '__main__':
    users = MongoSpiderAccountOper.get_spider_accounts(alive=100, task_number=1)
    print(len(users))
    random.shuffle(users)
    for index, user in enumerate(users):
        print(index, ' ', user.get('account'), user.get('proxies'))
        login(user)
        time.sleep(random.randint(30, 60))
