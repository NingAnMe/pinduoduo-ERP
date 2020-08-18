#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020-07-29 10:44
# @Author  : NingAnMe <ninganme@qq.com>
"""
ChromeDriver：https://sites.google.com/a/chromium.org/chromedriver/home
Selenium操作：https://zhuanlan.zhihu.com/p/111859925
Selenium配置：https://zhuanlan.zhihu.com/p/60852696
Selenium检测：https://mp.weixin.qq.com/s?__biz=MzI2MzEwNTY3OQ==&mid=2648978304&idx=1&sn=bff6a1f03b29702f2393a9fef9a50452&scene=21#wechat_redirect
Selenium获取Network请求和响应: https://cloud.tencent.com/developer/article/1549872
proxy：https://github.com/lightbody/browsermob-proxy/releases
"""
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
options = Options()
# options.add_experimental_option("debuggerAddress", "127.0.0.1:9515")
options.add_experimental_option('excludeSwitches', ['enable-automation'])
options.add_experimental_option('useAutomationExtension', False)
print('打开浏览器')
driver = webdriver.Chrome(executable_path=os.path.abspath("chromedriver_win32/chromedriver.exe"), options=options)

print('最大化窗口')
driver.maximize_window()
# driver.set_window_size(1080, 720)
driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
  "source": """
    Object.defineProperty(navigator, 'webdriver', {
      get: () => undefined
    })
  """
})

driver.get("https://mms.pinduoduo.com/")
# driver.get_screenshot_as_file("baidu_img.png")

# # 点击登录
# driver.find_element_by_xpath(r'//*[@id="root"]/div/div/div/main/div/section[2]/div/div/div/div[1]/div/div[2]').click()
#
# # 输入账号
# driver.find_element_by_xpath(r'//*[@id="usernameId"]').send_keys("13731111006")
# # 输入密码
# driver.find_element_by_xpath(r'//*[@id="passwordId"]').send_keys('An135321355')
# # 登录
# driver.find_element_by_xpath(r'//*[@id="root"]/div/div/div/main/div/section[2]/div/div/div/div[2]/section/div/div[2]/button').click()
# print(1)
