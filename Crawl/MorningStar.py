import threading
from __init__ import options
from selenium import webdriver
import os
import pandas as pd
import copy
import time
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from Crawl.Base.URLs import MorningStar as URL
from Crawl.PATH_SAVE import FOLDER_SAVE

# ===========================================================================

def check_crdownload(list_dir):
    for path in list_dir:
        if path.endswith(".crdownload"):
            return True

    return False


class Financial:
    def __init__(self, PATH_SAVE=FOLDER_SAVE, id=0, wait_after_click=3) -> None:
        PATH_SAVE = os.path.abspath(PATH_SAVE)
        self.PATH_SAVE = PATH_SAVE
        self.folder_F0 = PATH_SAVE + "\\Financial\\MorningStar\\F0"
        self.TEMP_FOLDER = PATH_SAVE + f"\\Financial\\MorningStar\\Temp\\{id}"
        if not os.path.exists(self.TEMP_FOLDER):
            os.mkdir(self.TEMP_FOLDER)

        self.options = copy.deepcopy(options)
        self.options.add_experimental_option("prefs", {"download.default_directory": self.TEMP_FOLDER})
        self.sleep_time = wait_after_click

    def download_data(self, symbol, driver):
        url = URL.FINANCIAL_REPORT.replace("__SYMBOL__", str(symbol))
        # driver = webdriver.Edge(self.options)
        driver.get(url)
        time.sleep(self.sleep_time)

        n = len(os.listdir(self.TEMP_FOLDER))

        # show all
        driver.find_element(By.XPATH, '//*[@id="__layout"]/div/div/div[2]/div[3]/div/main/div/div/div[1]/section/sal-components/div/sal-components-stocks-financials/div/div/div/div/div/div/div[2]/div[2]/div/div/a/span[2]').click()
        time.sleep(self.sleep_time)

        # download income statement
        driver.find_element(By.XPATH, '//*[@id="__layout"]/div/div/div[2]/div[3]/div/main/div/div/div[1]/section/sal-components/div/sal-components-stocks-financials/div/div/div/div/div/div/div[2]/div[1]/div[2]/div/div/div/div/div/div/div/div[2]/div[2]/div[2]/button').click()
        a = time.time()
        list_dir = os.listdir(self.TEMP_FOLDER)
        while len(list_dir) == n or check_crdownload(list_dir):
            time.sleep(1)
            if time.time() - a >= self.sleep_time:
                break

            list_dir = os.listdir(self.TEMP_FOLDER)

        n = len(os.listdir(self.TEMP_FOLDER))

        # balance sheet
        driver.find_element(By.XPATH, '//*[@id="balanceSheet"]').click()
        time.sleep(self.sleep_time)

        # download balance sheet
        driver.find_element(By.XPATH, '//*[@id="__layout"]/div/div/div[2]/div[3]/div/main/div/div/div[1]/section/sal-components/div/sal-components-stocks-financials/div/div/div/div/div/div/div[2]/div[1]/div[2]/div/div/div/div/div/div/div/div[2]/div[2]/div[2]/button').click()
        a = time.time()
        list_dir = os.listdir(self.TEMP_FOLDER)
        while len(list_dir) == n or check_crdownload(list_dir):
            time.sleep(1)
            if time.time() - a >= self.sleep_time:
                break

            list_dir = os.listdir(self.TEMP_FOLDER)

        # driver.quit()

    def _download_thread(self, thread_id):
        # driver = webdriver.Edge(self.list_crawler[thread_id].options)
        driver_on = False
        count = 0
        while True:
            self.lock.acquire()
            try:
                index = self.last_index
                self.last_index += 1
            finally: self.lock.release()

            if index >= self.num_com:
                break

            sym = self.df_code.index[index]
            check = self.df_code.loc[sym, "Check"]
            if type(check) == str and check == "Done":
                continue

            if not driver_on:
                driver = webdriver.Edge(self.list_crawler[thread_id].options)
                driver_on = True

            try:
                self.list_crawler[thread_id].download_data(sym, driver)
            except:
                pass

            print(index, sym, flush=True)
            count += 1
            if count == 20:
                driver.quit()
                driver_on = False
                count = 0

        if driver_on:
            driver.quit()

    def download_all(self, num_thread=8, MAX_TRIAL=5):
        print("===== Kéo báo cáo tài chính MorningStar =====", flush=True)
        self.list_crawler = [Financial(self.PATH_SAVE, i) for i in range(num_thread)]
        # self.list_browser = [webdriver.Edge(crawler.options) for crawler in self.list_crawler]
        self.df_code = pd.read_csv(self.PATH_SAVE + "\\List_com\\list_code.csv")
        self.df_code.set_index("Symbol", inplace=True)
        self.lock = threading.Lock()
        self.num_com = len(self.df_code)

        for trial in range(MAX_TRIAL):
            print("Lần", trial+1, flush=True)
            self.move_files()
            self.last_index = 0
            self.df_code["Check"] = pd.NA
            list_dir = os.listdir(self.folder_F0)
            for sym in self.df_code.index:
                if f"{sym}_balance.csv" in list_dir and f"{sym}_income.csv" in list_dir:
                    self.df_code.loc[sym, "Check"] = "Done"

            threads = []
            for i in range(num_thread):
                thread = threading.Thread(target=self._download_thread, args=(i, ))
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

            for crawler in self.list_crawler:
                crawler.sleep_time += 1

        self.move_files()
        self.df_code["Check"] = pd.NA
        list_dir = os.listdir(self.folder_F0)
        for sym in self.df_code.index:
            if f"{sym}_balance.csv" in list_dir and f"{sym}_income.csv" in list_dir:
                self.df_code.loc[sym, "Check"] = "Done"

        self.df_code.to_csv(self.folder_F0 + "\\check.csv")
        # for browser in self.list_browser:
        #     browser.quit()

        print("Xong", flush=True)

    def move_files(self):
        for temp_folder in os.listdir(self.PATH_SAVE + "\\Financial\\MorningStar\\Temp"):
            folder_path = self.PATH_SAVE + f"\\Financial\\MorningStar\\Temp\\{temp_folder}"
            list_file = os.listdir(folder_path)
            for file in list_file:
                xls = pd.ExcelFile(folder_path + f"\\{file}")
                sheetX = xls.parse(0)
                str1 = list(sheetX.columns)[0]
                re = str1[0:str1.find('-')]
                sheetX.to_csv(self.folder_F0 + f"\\{re}.csv", index=False)
                del xls
                os.remove(folder_path + f"\\{file}")
