import os
from datetime import datetime

# Test_mode: Lưu lại N công ty ngẫu nhiên và crawl dữ liệu
TEST_MODE = True
N = 100

# Đường dẫn đến folder ingestion
INGESTION_PATH = "C:\\Users\\AnnNg\\OneDrive\\Desktop\\DataJP\\Ingestion"

# Chỉnh ngày crawl hoặc để "AUTO", nhớ comment cái còn lại
# CRAWL_DATE = "2023/09/01"
CRAWL_DATE = "AUTO"

# Chỉnh những phần cần Crawl và thứ tự, Price phải được crawl trước Dividend
CRAWL_ORDER = ["Financial", "Price", "Dividend", "Volume"]

# Chỉnh số luồng thực hiện Crawl
NUM_THREAD = 6

# ===========================================================================

if CRAWL_DATE == "AUTO":
    now = datetime.now()
    if now.month < 10:
        month = "0" + str(now.month)
    else:
        month = str(now.month)

    if now.day < 10:
        day = "0" + str(now.day)
    else:
        day = str(now.day)

    CRAWL_DATE = f"{now.year}/{month}/{day}"

FOLDER_SAVE = f"{INGESTION_PATH}\\{'_'.join(CRAWL_DATE.split('/'))}"


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

def create_folders():
    os.makedirs(FOLDER_SAVE + "\\List_com", exist_ok=True)
    create_folder_financial()
    create_folder_price()
    create_folder_dividend()
    create_folder_volume()
