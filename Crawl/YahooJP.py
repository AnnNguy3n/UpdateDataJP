import threading
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import pandas as pd
from Crawl.Base.URLs import YahooJP as URL
import os
from __init__ import options
import time
from Crawl.PATH_SAVE import DATE_CRAWL, FOLDER_SAVE

# ===========================================================================

class PriceClosed:
    def __init__(self, num_year=2, wait_after_click=1) -> None:
        max_time = DATE_CRAWL
        min_time = str(int(DATE_CRAWL[:4]) - num_year) + DATE_CRAWL[4:]
        self.URL = URL.PRICE_CLOSED.replace("__START_DATE__", "".join(min_time.split("/")))\
                    .replace("__END_DATE__", "".join(max_time.split("/")))
        self.sleep_time = wait_after_click

    def get_price_history(self, symbol, driver: webdriver.Edge):
        main_url = self.URL.replace("__SYMBOL__", str(symbol)).replace("__PAGE__", "1")
        try:
            driver.get(main_url)
            time.sleep(self.sleep_time)

            num_row = driver.find_element(By.XPATH, '//*[@id="pagerbtm"]/p')
            num_row = int(num_row.text[num_row.text.index('/')+1:-1])

            ul_button = driver.find_element(By.XPATH, '//*[@id="pagerbtm"]/ul')
            buttons = ul_button.find_elements(By.TAG_NAME, "li")
            next_button_id = len(buttons)
            next_button_xpath = f'//*[@id="pagerbtm"]/ul/li[{next_button_id}]/button'

            while True:
                soup = BeautifulSoup(driver.page_source, "html.parser")
                table = soup.find("table", attrs={"class": "_13C_m5Hx _1aNPcH77"})
                try:
                    df = pd.read_html(str(table))[0]
                except:
                    break

                try:
                    data = pd.concat([data, df], ignore_index=True)
                except:
                    data = df.copy()

                try:
                    button = driver.find_element(By.XPATH, next_button_xpath)
                    button.click()
                    time.sleep(self.sleep_time)
                except:
                    break

            if len(data) == num_row:
                return True, data

            temp = data.copy()
            temp["Check"] = pd.to_numeric(temp["終値"], errors="coerce")
            temp = temp[temp["Check"].notna()]
            if len(temp) == num_row:
                return True, data

            return False, data
        except Exception as exception:
            return False, f"Error: {exception}"

    def _get_all_data_thread(self):
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

            sym = str(self.df_code.loc[index, "Symbol"])
            if type(self.check_list.loc[index, "Check"]) == str and self.check_list.loc[index, "Check"] == "Done":
                continue

            if not driver_on:
                driver = webdriver.Edge(options)
                driver_on = True

            check, table = self.get_price_history(sym, driver)
            if not check:
                self.lock.acquire()
                try:
                    if type(table) == str:
                        self.check_list.loc[index, "Check"] = "Không lấy được giá"
                    elif type(table) == pd.DataFrame:
                        self.check_list.loc[index, "Check"] = "Lấy không đúng số lượng dòng"
                        table.to_csv(self.folder_F0 + f"\\{sym}.csv", index=False)
                    else:
                        self.check_list.loc[index, "Check"] = "Lỗi không xác định"

                    self.check_list.to_csv(self.folder_F0 + "\\check.csv", index=False)
                finally: self.lock.release()
                print(index, sym, "error", flush=True)
            else:
                table.to_csv(self.folder_F0 + f"\\{sym}.csv", index=False)
                self.lock.acquire()
                try:
                    self.check_list.loc[index, "Check"] = "Done"
                    self.check_list.to_csv(self.folder_F0 + "\\check.csv", index=False)
                finally: self.lock.release()
                print(index, sym, "done", flush=True)

            count += 1
            if count == 20:
                driver.quit()
                driver_on = False
                count = 0

        if driver_on:
            driver.quit()

    def get_all_data(self, PATH_SAVE=FOLDER_SAVE, num_thread=8, MAX_TRIAL=5):
        print("===== Kéo giá YahooJP =====", flush=True)
        PATH_SAVE = os.path.abspath(PATH_SAVE)
        self.df_code = pd.read_csv(PATH_SAVE + "\\List_com\\list_code.csv")
        self.folder_F0 = PATH_SAVE + "\\Price\\YahooJP\\F0"

        try:
            self.check_list = pd.read_csv(self.folder_F0 + "\\check.csv")
        except:
            self.check_list = self.df_code[["Symbol"]].copy()
            self.check_list["Check"] = pd.NA
            self.check_list.to_csv(self.folder_F0 + "\\check.csv", index=False)

        self.lock = threading.Lock()
        self.num_com = len(self.check_list)

        for trial in range(MAX_TRIAL):
            print("Lần", trial+1, flush=True)
            self.last_index = 0
            threads = []
            for i in range(num_thread):
                thread = threading.Thread(target=self._get_all_data_thread, args=())
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

            self.sleep_time += 0.5

        print("Xong", flush=True)

# ===========================================================================

class Dividend:
    def __init__(self, PATH_SAVE=FOLDER_SAVE) -> None:
        PATH_SAVE = os.path.abspath(PATH_SAVE)
        self.folder_F0_price = PATH_SAVE + "\\Price\\YahooJP\\F0"
        self.list_path = os.listdir(self.folder_F0_price)
        self.folder_save = PATH_SAVE + "\\Dividend\\YahooJP\\F0"

    def get_all_dividend(self):
        try:
            pd.read_csv(self.folder_save + "\\all_dividend.csv")
            print("File dividend (YahooJP) đã có từ trước nên không chạy nữa", flush=True)
            return
        except:
            pass
        
        print("===== Lấy Dividend từ Price YahooJP =====", flush=True)
        df_rs = pd.DataFrame({"Symbol": [], "Time": [], "PriceClosed": [], "VolumeTrade": []})
        list_error = []
        for jj in range(len(self.list_path)):
            path = self.list_path[jj]
            try:
                if path != "check.csv":
                    data = pd.read_csv(self.folder_F0_price+"\\"+path)[["日付", "終値", "出来高"]]
                    data.columns = ["Time", "PriceClosed", "VolumeTrade"]
                    data["Check"] = pd.to_numeric(data["PriceClosed"], errors="coerce")
                    temp = data[data["Check"].isna()].copy()
                    temp.pop("Check")
                    temp["Symbol"] = path.split(".")[0]
                    df_rs = pd.concat([df_rs, temp[["Symbol", "Time", "PriceClosed", "VolumeTrade"]]], ignore_index=True)
                    print(jj, path, "Done", flush=True)
            except:
                list_error.append(path)
                print(jj, path, "Error", flush=True)
        
        df_rs["temp"] = df_rs["PriceClosed"].str.split("分割：").str.get(1)
        df_rs["old"] = df_rs["temp"].str.split("株→").str.get(0)
        df_rs["new"] = df_rs["temp"].str.split("株→").str.get(1).str.split("株").str.get(0)
        df_rs["Splits"] = df_rs["new"].combine(df_rs["old"], lambda x, y: x + "/" + y)
        df_save = df_rs[["Symbol", "Time", "PriceClosed", "Splits"]].copy()
        df_save.rename(columns={"PriceClosed": "Content"}, inplace=True)
        df_save.to_csv(self.folder_save + "\\all_dividend.csv", index=False)
        pd.DataFrame({"Error": list_error}).to_csv(self.folder_save + "\\error.csv", index=False)
        print("Xong", flush=True)