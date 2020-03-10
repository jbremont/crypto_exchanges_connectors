import os
import random

from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.remote.command import Command
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
#import pyotp

class LoginPage(object):
    def __init__(self, driver=None):
        self.url = 'https://www.hotbit.io/login'

        if driver is not None:
            self.driver = driver
        else:
            opts = Options()
            opts.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/66.0.3359.181 Chrome/66.0.3359.181 Safari/537.36")
            #capabilities = webdriver.DesiredCapabilities().FIREFOX
            #capabilities["marionette"] = False
            #self.driver = webdriver.Firefox(capabilities=capabilities)
            opts.add_argument("disable-infobars");
            #opts.add_argument("user-data-dir=/home/jsterling8/.config/chromium/Profile 1")
            #self.driver = webdriver.Chrome(executable_path="/home/jsterling8/Dropbox/git/aye-jay/exchanges/exchanges/hotbit/chromedriver", options=opts)
            self.driver = webdriver.Chrome(executable_path="/usr/lib/chromium-browser/chromedriver", options=opts)

    def open(self):
        try:
            self.driver.get(self.url)
            WebDriverWait(self.driver, 15).until(EC.presence_of_element_located((By.ID, 'nc_1_n1z')))
        except TimeoutException:
            print('Page didn\'t load in 15 seconds')


    def get_profile(self):
        profile = webdriver.FirefoxProfile()
        profile.set_preference("browser.privatebrowsing.autostart", True)
        return profile

    def login(self, username, password, two_fa_seed):

        self.driver.find_element_by_id('username').send_keys(username)
        self.driver.find_element_by_id('password').send_keys(password)
        self.drag_slider()
        self.driver.find_element_by_id('loginbutton').click()
        self.enter_2fa_code(two_fa_seed)

    def drag_slider(self):
        slider = self.driver.find_element_by_id('nc_1_n1z')
        ac = ActionChains(self.driver)
        ac.move_to_element(slider)
        ac.click_and_hold()
        total_x_move = 0
        total_x_offset = random.randint(333, 567)
        while total_x_move < 4000 + total_x_offset:
            x_move = random.randint(50,250)
            y_move = random.randint(-3,3)
            ac.move_by_offset(x_move, y_move)
            total_x_move += x_move
        ac.release()
        ac.perform()
        #ActionChains(self.driver).drag_and_drop_by_offset(slider, 500, 5).perform()

    def enter_2fa_code(self, two_fa_seed):
        code = '123456' #totp = pyotp.TOTP(two_fa_seed)
        self.driver.find_element_by_id('googleauth').send_keys(code)

if __name__ == '__main__':
    lp = LoginPage()
    lp.open()
    lp.login('jon@thancodes.com', 'DGsq6t@FqaD84Z2f', 'TWO_FA_SEED')
