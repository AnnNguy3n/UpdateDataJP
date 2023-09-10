import os
from datetime import datetime

# Test mode: Lưu lại n công ty ngẫu nhiên và crawl dữ liệu
TEST_MODE = False
N = 100

# Đường dẫn của folder ingestion và ngày crawl
PATH_INGESTION = "C:\\Users\\AnnNg\\OneDrive\\Desktop\\Data_Nhat\\UpdateDataJapan\\SAVE"
DATE_CRAWL = "2023/09/06"
# DATE_CRAWL = "AUTO"

# Chỉnh thứ tự Crawl
# Price phải được crawl trước dividend
CRAWL_ORDER = ["Financial", "Price", "Dividend", "Volume"]

# Chỉnh số luồng tại đây
NUM_THREAD = 6

# ===========================================================================

FOLDER_SAVE = f'{PATH_INGESTION}\\{"_".join(DATE_CRAWL.split("/"))}'


def create_folder_financial():
    folder_financial = FOLDER_SAVE + "\\Financial"

    IrBank_F0_BA = folder_financial + "\\IrBank\\F0\\Balance"
    IrBank_F0_IC = folder_financial + "\\IrBank\\F0\\Income"
    os.makedirs(IrBank_F0_BA, exist_ok=True)
    os.makedirs(IrBank_F0_IC, exist_ok=True)

    MorningStar_F0 = folder_financial + "\\MorningStar\\F0"
    os.makedirs(MorningStar_F0, exist_ok=True)

    MorningStar_temp = folder_financial + "\\MorningStar\\Temp"
    os.makedirs(MorningStar_temp, exist_ok=True)

def create_folder_price():
    folder_price = FOLDER_SAVE + "\\Price"

    YahooJP_F0 = folder_price + "\\YahooJP\\F0"
    os.makedirs(YahooJP_F0, exist_ok=True)

def create_folder_dividend():
    folder_dividend = FOLDER_SAVE + "\\Dividend"

    Kabu_F0 = folder_dividend + "\\Kabu\\F0"
    os.makedirs(Kabu_F0, exist_ok=True)

    YahooJP_F0 = folder_dividend + "\\YahooJP\\F0"
    os.makedirs(YahooJP_F0, exist_ok=True)

def create_folder_volume():
    folder_volume = FOLDER_SAVE + "\\Volume"

    MorningStar_F0 = folder_volume + "\\MorningStar\\F0"
    os.makedirs(MorningStar_F0, exist_ok=True)

    Buffet_F0 = folder_volume + "\\Buffet\\F0"
    os.makedirs(Buffet_F0, exist_ok=True)

def create_folder():
    os.makedirs(FOLDER_SAVE + "\\List_com", exist_ok=True)
    create_folder_financial()
    create_folder_price()
    create_folder_dividend()
    create_folder_volume()
