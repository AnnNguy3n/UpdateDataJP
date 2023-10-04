import time
import re
import copy
import threading
import os
import pandas as pd
import numpy as np
from selenium import webdriver
from bs4 import BeautifulSoup
from __init__ import options
from Crawl.PATH_SAVE import FOLDER_SAVE
from Crawl.Base.URLs import Buffet as URL


class GetProxyDriver:
    def __init__(self) -> None:
        self.urls = [
                    # 'https://www.proxynova.com/proxy-server-list',
                    # 'https://www.proxynova.com/proxy-server-list/country-vn',
                    # 'https://www.proxynova.com/proxy-server-list/country-cn',
                    'https://www.proxynova.com/proxy-server-list/country-th',
                    ]

    def getProxyTable(self, MAX_TRIAL=5):
        driver = webdriver.Edge(options)
        all_proxy = None
        for url in self.urls:
            sleep_time = 2
            for trial in range(MAX_TRIAL):
                try:
                    driver.get(url)
                    time.sleep(sleep_time)
                    html = driver.page_source
                    soup = BeautifulSoup(html, 'html.parser')
                    tables = soup.find('table', id='tbl_proxy_list')
                    tbody = tables.find('tbody')

                    pattern = r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
                    ip_address = re.findall(pattern, tbody.text)

                    df_proxy = pd.read_html(str(tables))[0].dropna(how = 'all')
                    df_proxy['Proxy IP'][:len(ip_address)] = ip_address
                    if all_proxy is None:
                        all_proxy = df_proxy.copy()
                    else:
                        all_proxy = pd.concat([all_proxy, df_proxy], ignore_index=True)

                    print(url, "Done")
                    break
                except:
                    print(url, "Error")
                    sleep_time += 1

        list_proxy = all_proxy["Proxy IP"].combine(all_proxy["Proxy Port"], lambda x,y: f"{x}:{int(y)}")
        return list_proxy.to_list()

    def checkDriver(self, PROXY):
        """
        Check if the driver can access to the website

        Parameters
        ----------
        PROXY : str
            Proxy IP and port

        Returns
        -------
        chrome : selenium.webdriver.chrome.webdriver.WebDriver
            Chrome driver
        """
        EdgeOptions = copy.deepcopy(options)
        EdgeOptions.add_argument('--proxy-server=%s' % PROXY)

        egde = webdriver.Edge(EdgeOptions)
        # egde.implicitly_wait(10)
        try:
            egde.get('https://www.buffett-code.com/')
            time.sleep(3)
        except:
            print(PROXY, "Error")
            egde.quit()
            return None

        page_source = egde.page_source
        if ('バフェット・コード' in page_source) and ("403 Forbidden" not in page_source):
            return egde

        egde.quit()
        return None

# ===========================================================================

class Volume:
    def __init__(self, PATH_SAVE=FOLDER_SAVE, wait_after_click=4) -> None:
        PATH_SAVE = os.path.abspath(PATH_SAVE)
        self.PATH_SAVE = PATH_SAVE
        self.folder_F0 = PATH_SAVE + "\\Volume\\Buffet\\F0"
        self.sleep_time = wait_after_click
        self.get_proxy_driver = GetProxyDriver()
        try:
            pd.read_csv(PATH_SAVE + "\\Volume\\Buffet\\list_proxy.csv")
        except:
            list_proxy = self.get_proxy_driver.getProxyTable()
            pd.DataFrame({"proxy": list_proxy, "BeingUsed": 0}).to_csv(PATH_SAVE + "\\Volume\\Buffet\\list_proxy.csv")

        self.df_proxy = pd.read_csv(PATH_SAVE + "\\Volume\\Buffet\\list_proxy.csv")
        self.df_proxy.set_index(self.df_proxy.columns[0], inplace=True)
        self.df_proxy["BeingUsed"] = 0
        self.df_proxy["Fail"] = 0

    def get_volume(self, symbol, driver: webdriver.Edge):
        url = URL.VOLUME.replace("__SYMBOL__", str(symbol))
        try:
            driver.get(url)
            time.sleep(self.sleep_time)

            soup = BeautifulSoup(driver.page_source, "html.parser")
            tables = soup.find_all("table", attrs={"class": "table table-striped table-bordered table-condensed"})

            for t_ in tables:
                if "株数" in t_.text:
                    table = t_
                    break
            else:
                table = None

            if table is None:
                return False, "Không tìm thấy bảng chứa volume"

            df = pd.read_html(str(table))[0]
            return True, df[df[0].str.contains("株数")].iloc[0, 1]
        except Exception as exception:
            return False, f"Error: {exception}"

    def _get_all_volume_thread(self):
        driver_on = False
        while True:
            self.lock.acquire()
            try:
                index = self.last_index
                self.last_index += 1
            finally: self.lock.release()

            if index >= self.num_com:
                break

            sym = self.df_rs.index[index]
            check = self.df_rs.loc[sym, "Check"]
            if type(check) == str and check == "Done":
                continue

            if not driver_on:
                self.lock.acquire()
                try:
                    temp = self.df_proxy[self.df_proxy["BeingUsed"]==0]
                    min_fail = temp[temp["Fail"] == temp["Fail"].min()]
                    proxy_id = np.random.choice(min_fail.index)
                    proxy = self.df_proxy.loc[proxy_id, "proxy"]
                    self.df_proxy.loc[proxy_id, "BeingUsed"] = 1
                except:
                    print("Đã có lỗi xảy ra", flush=True)
                    return
                finally: self.lock.release()

                self.lock.acquire()
                try: driver = self.get_proxy_driver.checkDriver(proxy)
                finally: self.lock.release()
                while driver is None:
                    self.lock.acquire()
                    try:
                        self.df_proxy.loc[proxy_id, "Fail"] += 1
                        self.df_proxy.loc[proxy_id, "BeingUsed"] = 0
                        temp = self.df_proxy[self.df_proxy["BeingUsed"]==0]
                        min_fail = temp[temp["Fail"] == temp["Fail"].min()]
                        proxy_id = np.random.choice(min_fail.index)
                        proxy = self.df_proxy.loc[proxy_id, "proxy"]
                        self.df_proxy.loc[proxy_id, "BeingUsed"] = 1
                    except:
                        print("Đã có lỗi xảy ra", flush=True)
                        return
                    finally: self.lock.release()

                    self.lock.acquire()
                    try: driver = self.get_proxy_driver.checkDriver(proxy)
                    finally: self.lock.release()

                driver_on = True

            check, vol = self.get_volume(sym, driver)
            time.sleep(self.sleep_time)
            self.lock.acquire()
            try:
                if check:
                    self.df_rs.loc[sym, "Check"] = "Done"
                    self.df_rs.loc[sym, "Volume"] = vol
                else:
                    self.df_rs.loc[sym, "Check"] = "Error"

                self.df_rs.to_csv(self.folder_F0 + "\\volume.csv")
            finally: self.lock.release()
            print(index, sym, flush=True)
            if not check:
                self.lock.acquire()
                try: self.df_proxy.loc[proxy_id, "BeingUsed"] = 0
                finally: self.lock.release()
                driver.quit()
                driver_on = False

        if driver_on:
            self.lock.acquire()
            try: self.df_proxy.loc[proxy_id, "BeingUsed"] = 0
            finally: self.lock.release()
            driver.quit()

    def get_all_volume(self, num_thread=8, MAX_TRIAL=5):
        print("===== Kéo volume Buffet =====", flush=True)
        try:
            df_rs = pd.read_csv(self.folder_F0 + "\\volume.csv")
        except:
            df_code = pd.read_csv(self.PATH_SAVE + "\\List_com\\list_code.csv")
            df_rs = df_code.copy()
            df_rs["Check"] = pd.NA
            df_rs["Volume"] = pd.NA

        df_rs.set_index("Symbol", inplace=True)
        self.df_rs = df_rs
        self.lock = threading.Lock()
        self.num_com = len(self.df_rs)

        for trial in range(MAX_TRIAL):
            print("Lần", trial+1, flush=True)
            self.last_index = 0

            threads = []
            for i in range(num_thread):
                thread = threading.Thread(target=self._get_all_volume_thread, args=())
                threads.append(thread)
                thread.start()
                time.sleep(self.sleep_time)

            for thread in threads:
                thread.join()

            self.sleep_time += 2

        print("Xong", flush=True)
