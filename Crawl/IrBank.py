import pandas as pd
import requests
from bs4 import BeautifulSoup
import threading
import os
from __init__ import options
from selenium import webdriver
from Crawl.Base.Codes.Financial_IrBank import link_main_crawler, IrbankCrawler
from Crawl.Base.URLs import IrBank as URL
from Crawl.PATH_SAVE import DATE_CRAWL, FOLDER_SAVE

# ===========================================================================

class ListCompany:
    def __init__(self) -> None:
        pass

    def get_coms_from_1_sec(self, sector, session: requests.Session):
        url = URL.LIST_COMPANY.replace("__SECTOR__", str(sector))
        try:
            r = session.get(url)
            status_code = r.status_code
            if status_code == 200:
                soup = BeautifulSoup(r.content, "html.parser")
                table = soup.find("table", attrs={"id": "code1"})
                df = pd.read_html(str(table))[0][["No.", "時価総額"]]
                temp_loc = []
                for i in df.index:
                    try:
                        int(df.loc[i, "No."])
                        temp_loc.append(i)
                    except:
                        pass

                df = df.loc[temp_loc]
                df["Sector"] = sector
                df = df[df["No."].notna() & df["時価総額"].notna()]
                return True, df

            return False, f"Error: {status_code}"
        except Exception as exception:
            return False, f"Error: {exception}"

    def get_coms_from_all_sector(self, MAX_TRIAL=5):
        print("===== Kéo danh sách công ty =====", flush=True)
        list_sector = pd.read_csv(URL.PATH_LIST_SECTOR)["Sector"]
        df_count = pd.DataFrame({"Sector": list_sector, "Count": pd.NA})
        df_count.set_index("Sector", inplace=True)

        session = requests.Session()
        for trial in range(MAX_TRIAL):
            print("Lần", trial+1, flush=True)
            for sector in list_sector:
                if type(df_count.loc[sector, "Count"]) == int:
                    continue

                check, temp = self.get_coms_from_1_sec(sector, session)
                if check:
                    try: data = pd.concat([data, temp], ignore_index=True)
                    except: data = temp
                    df_count.loc[sector, "Count"] = len(temp)
                else:
                    df_count.loc[sector, "Count"] = temp

        data = data.rename(columns={"No.": "Symbol", "時価総額": "MarketCap"})
        print("Xong", flush=True)
        session.close()
        return data, df_count

    def get_company_code(self, symbol, session: requests.Session):
        url = URL.FINANCIAL_REPORT.replace("__CODE__", str(symbol))
        try:
            r = session.get(url)
            status_code = r.status_code
            if status_code == 200:
                code = r.url.split("/")[3]
                return True, code

            return False, f"Error: {status_code}"
        except Exception as exception:
            return False, f"Error: {exception}"

    def _get_all_company_code_thread(self):
        session = requests.Session()
        while True:
            self.lock.acquire()
            try:
                start = self.last_index
                self.last_index += self.len_block
                end = self.last_index
            finally: self.lock.release()

            if start >= self.len_data:
                break

            df_temp = self.df_symbol.loc[start:end-1]
            df_temp = df_temp.loc[(df_temp["Code"].isna()) | (df_temp["Code"].str.startswith("Error: "))]
            if len(df_temp) > 0:
                df_temp["Code"] = df_temp["Symbol"].apply(lambda x: self.get_company_code(x, session)[1])
                self.lock.acquire()
                try: self.df_symbol.loc[df_temp.index] = df_temp
                finally: self.lock.release()
        
        session.close()

    def get_all_company_code(self, df_symbol, len_block=30, num_thread=8, MAX_TRIAL=5):
        print("===== Lấy mã code cho các công ty =====", flush=True)
        self.lock = threading.Lock()
        self.len_block = len_block
        self.df_symbol = df_symbol
        self.len_data = len(df_symbol)
        self.df_symbol["Code"] = pd.NA

        for trial in range(MAX_TRIAL):
            print("Lần", trial + 1, flush=True)
            self.last_index = 0
            threads = []
            for i in range(num_thread):
                thread = threading.Thread(target=self._get_all_company_code_thread, args=())
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

        print("Xong", flush=True)
        return self.df_symbol

# ===========================================================================

class Financial:
    def __init__(self, num_year=2) -> None:
        self.crawler = IrbankCrawler.IrbankCrawler("edge")
        self.max_time = DATE_CRAWL[:-3]
        self.min_time = str(int(DATE_CRAWL[:4]) - num_year) + DATE_CRAWL[4:-3]

    def get_doc_codes(self, code, driver):
        try:
            table = link_main_crawler.getTableOfLinks(code, drop_cols=["修正等", "1Q", "2Q", "3Q"])[["通期"]]
            table = table[(table.index.str[:] >= str(self.min_time)) & (table.index.str[:] <= str(self.max_time))]
            table = URL.FINANCIAL_REPORT.replace("__CODE__", str(code)).replace("reports", "") + table

            for col in table.columns:
                financial_report = table[col]
                i = 0
                finan_length = len(financial_report)
                while i < finan_length:
                    max_prev_links = finan_length - i - 1
                    root_link = financial_report[i]
                    prev_links = link_main_crawler.getPrevLinks(root_link, driver, max_prev_links)
                    if prev_links:
                        financial_report[i+1:i+1+len(prev_links)] = prev_links
                        i += 1 + len(prev_links)
                    else:
                        i += 1
                table[col] = financial_report

            return True, table
        except Exception as exception:
            return False, f"Error: {exception}"

    def get_data(self, code, doc_codes, driver, report_type):
        try:
            data = self.crawler.getData(code, doc_codes, driver, report_type)
            return True, data
        except Exception as exception:
            return False, f"Error: {exception}"

    def _get_all_data_thread(self, thread_id):
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
            code = self.df_code.loc[index, "Code"]
            if code.startswith("Error"):
                continue

            if type(self.check_list.loc[index, "Check"]) == str and self.check_list.loc[index, "Check"] == "Done":
                continue

            if not driver_on:
                driver = webdriver.Edge(options)
                driver.set_page_load_timeout(60)
                driver_on = True

            check, table = self.list_crawler[thread_id].get_doc_codes(code, driver)
            if not check:
                self.lock.acquire()
                try:
                    self.check_list.loc[index, "Check"] = "Không lấy được doc_codes"
                    self.check_list.to_csv(self.folder_F0 + "\\check.csv", index=False)
                finally: self.lock.release()

                continue

            done_ba, data_ba = self.list_crawler[thread_id].get_data(code, table["通期"], driver, "bs")
            done_ic, data_ic = self.list_crawler[thread_id].get_data(code, table["通期"], driver, "pl")
            error = ""
            if not done_ba:
                error += f"Không lấy được balance, "
            else:
                data_ba.to_csv(self.folder_balance + f"\\{sym}.csv", index=False)

            if not done_ic:
                error += f"Không lấy được income, "
            else:
                data_ic.to_csv(self.folder_income + f"\\{sym}.csv", index=False)

            if error == "":
                error = "Done"

            self.lock.acquire()
            try:
                self.check_list.loc[index, "Check"] = error
                self.check_list.to_csv(self.folder_F0 + "\\check.csv", index=False)
            finally: self.lock.release()

            print(index, sym, code, flush=True)
            count += 1
            if count == 20:
                driver.quit()
                driver_on = False
                count = 0

        if driver_on:
            driver.quit()

    def get_all_data(self, PATH_SAVE=FOLDER_SAVE, MAX_TRIAL=5, num_thread=8):
        print("===== Kéo báo cáo tài chính IrBank =====", flush=True)
        PATH_SAVE = os.path.abspath(PATH_SAVE)
        self.df_code = pd.read_csv(PATH_SAVE + "\\List_com\\list_code.csv")
        self.folder_F0 = PATH_SAVE + "\\Financial\\IrBank\\F0"
        self.folder_balance = self.folder_F0 + "\\Balance"
        self.folder_income = self.folder_F0 + "\\Income"

        try:
            self.check_list = pd.read_csv(self.folder_F0 + "\\check.csv")
        except:
            self.check_list = self.df_code[["Symbol"]].copy()
            self.check_list["Check"] = pd.NA
            self.check_list.to_csv(self.folder_F0 + "\\check.csv", index=False)

        self.lock = threading.Lock()
        self.num_com = len(self.check_list)
        self.list_crawler = [Financial() for i in range(num_thread)]

        for trial in range(MAX_TRIAL):
            print("Lần", trial+1, flush=True)
            self.last_index = 0
            threads = []
            for i in range(num_thread):
                thread = threading.Thread(target=self._get_all_data_thread, args=(i, ))
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

        print("Xong", flush=True)