from selenium import webdriver
import pandas as pd
from bs4 import BeautifulSoup
import os
from Crawl.Base.URLs import Kabu as URL
from __init__ import options
from Crawl.PATH_SAVE import FOLDER_SAVE
import time
from datetime import timedelta

# ===========================================================================

class Dividend:
    def __init__(self, PATH_SAVE=FOLDER_SAVE) -> None:
        PATH_SAVE = os.path.abspath(PATH_SAVE)
        self.folder_F0 = PATH_SAVE + "\\Dividend\\Kabu\\F0"

    def get_data(self, url, driver, sleeptime):
        try:
            driver.get(url)
            time.sleep(sleeptime)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            table = soup.find("table", attrs={"class": "tbl01"})
            df = pd.read_html(str(table))[0]
            return True, df
        except Exception as exception:
            return False, f"Error: {exception}"

    def get_all_data(self, MAX_TRIAL=5):
        try:
            check = pd.read_csv(self.folder_F0 + "\\check.csv")
            if (check["Check"] == "Done").all():
                return
        except:
            check = pd.DataFrame({"table_id": [1, 2], "Check": pd.NA})

        print("===== Kéo dividend Kabu =====", flush=True)
        driver = webdriver.Edge(options)
        sleeptime = 4
        for k in range(MAX_TRIAL):
            check1, df_1 = self.get_data(URL.TABLE_DIVIDEND_1, driver, sleeptime)
            check2, df_2 = self.get_data(URL.TABLE_DIVIDEND_2, driver, sleeptime)
            if check1 and check2:
                break

            sleeptime += 2

        list_temp = []

        def split_1(value):
            value = value.replace(" ", "")
            temp = value.split("：")
            return temp[0] + "/" + temp[1]

        def split_2(value):
            value = value.replace(" ", "")
            value = value.replace("株", "")
            temp = value.split("→")
            return temp[1] + "/" + temp[0]

        if check1:
            df_1.columns = df_1.columns.str.replace(" ", "")
            df_1.to_csv(self.folder_F0 + "\\table_1.csv", index=False)
            check.loc[0, "Check"] = "Done"
            temp_1 = df_1[["銘柄コード", "割当比率", "権利付最終日"]].copy()
            temp_1.columns = ["Symbol", "Splits", "Time"]
            temp_1["Splits"] = temp_1["Splits"].apply(split_1)
            list_temp.append(temp_1)
        else:
            check.loc[0, "Check"] = "Error"

        if check2:
            df_2.columns = df_2.columns.str.replace(" ", "")
            df_2.to_csv(self.folder_F0 + "\\table_2.csv", index=False)
            check.loc[1, "Check"] = "Done"
            temp_2 = df_2[["銘柄コード", "併合比率", "権利付最終日"]].copy()
            temp_2.columns = ["Symbol", "Splits", "Time"]
            temp_2["Splits"] = temp_2["Splits"].apply(split_2)
            list_temp.append(temp_2)
        else:
            check.loc[1, "Check"] = "Error"

        check.to_csv(self.folder_F0 + "\\check.csv", index=False)
        print("Xong", flush=True)
        for df in list_temp:
            try:
                data = pd.concat([data, df], ignore_index=True)
            except:
                data = df.copy()

        data["Time"] = data["Time"].apply(lambda x: (pd.to_datetime(x) + timedelta(1)).strftime("%Y/%m/%d"))
        data.drop_duplicates().to_csv(self.folder_F0 + "\\All_dividend.csv", index=False)