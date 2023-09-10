from Crawl.PATH_SAVE import TEST_MODE, N, FOLDER_SAVE, NUM_THREAD, create_folders, CRAWL_ORDER
from Crawl.IrBank import ListCompany, Financial as IrFinancial
from Crawl.MorningStar import Financial as MorFinancial
from Crawl.YahooJP import PriceClosed as YaJpPriceClosed, Dividend as YaJpDividned
from Crawl.Kabu import Dividend as KabuDividend

import pandas as pd

# Tạo folder lưu
create_folders()

# Crawl company_codes
try:
    df_code = pd.read_csv(FOLDER_SAVE + "\\List_com\\list_code_full.csv")
    df_code_saved = pd.read_csv(FOLDER_SAVE + "\\List_com\\list_code.csv")
    if TEST_MODE:
        if len(df_code_saved) != N:
            df_code.sample(N).reset_index(drop=True).to_csv(FOLDER_SAVE + "\\List_com\\list_code.csv", index=False)
    else:
        if len(df_code_saved) != len(df_code):
            df_code.to_csv(FOLDER_SAVE + "\\List_com\\list_code.csv", index=False)
except:
    listCompany = ListCompany()
    df, count = listCompany.get_coms_from_all_sector()
    df.to_csv(FOLDER_SAVE + "\\List_com\\list_com.csv", index=False)
    count.to_csv(FOLDER_SAVE + "\\List_com\\count.csv")

    df_code = pd.DataFrame({"Symbol": df["Symbol"].unique()})
    df_code = listCompany.get_all_company_code(df_code, num_thread=NUM_THREAD)
    df_code.to_csv(FOLDER_SAVE + "\\List_com\\list_code_full.csv", index=False)
    if TEST_MODE:
        df_code.sample(N).reset_index(drop=True).to_csv(FOLDER_SAVE + "\\List_com\\list_code.csv", index=False)
    else:
        df_code.to_csv(FOLDER_SAVE + "\\List_com\\list_code.csv", index=False)

# Crawl
for name in CRAWL_ORDER:
    if name == "Financial":
        crl_1 = IrFinancial()
        crl_1.get_all_data(num_thread=NUM_THREAD)

        crl_2 = MorFinancial()
        crl_2.download_all(num_thread=NUM_THREAD)

    elif name == "Price":
        crl_3 = YaJpPriceClosed()
        crl_3.get_all_data(num_thread=NUM_THREAD)

    elif name == "Dividend":
        crl_4 = YaJpDividned()
        crl_4.get_all_dividend()

        crl_5 = KabuDividend()
        crl_5.get_all_data()