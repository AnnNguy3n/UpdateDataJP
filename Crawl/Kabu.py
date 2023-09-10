from selenium import webdriver
import pandas as pd
from bs4 import BeautifulSoup
import os
from Crawl.Base.URLs import Kabu as URL
from __init__ import options
from Crawl.PATH_SAVE import FOLDER_SAVE
import time

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

        if check1:
            df_1.to_csv(self.folder_F0 + "\\table_1.csv", index=False)
            check.loc[0, "Check"] = "Done"
        else:
            check.loc[0, "Check"] = "Error"

        if check2:
            df_2.to_csv(self.folder_F0 + "\\table_2.csv", index=False)
            check.loc[1, "Check"] = "Done"
        else:
            check.loc[1, "Check"] = "Error"

        check.to_csv(self.folder_F0 + "\\check.csv", index=False)
        print("Xong", flush=True)