import os
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from exchanges.hotbit.page_objects.login_page import LoginPage


class ExchangePage(object):
    def __init__(self, base, quote, driver=None):
        self.url = 'https://www.hotbit.io/exchange?symbol=' + base + '/' + quote
        if driver is not None:
            self.driver = driver
        else:
            self.driver = webdriver.Chrome("/usr/lib/chromium-browser/chromedriver")

    def open(self):
        self.driver.get(self.url)

    def click_login_button(self):
        login_button = self.driver.find_element_by_css_selector('a[href="/login"]')
        if login_button is not None:
            login_button.click()

        return LoginPage()


if __name__ == '__main__':
    ep = ExchangePage('UBT', 'BTC')
    ep.open()
    ep.login()