import pandas as pd
import numpy as np
import re
from __init__ import ROOT_PATH


# ====================================================================================================

class MorningStar:
    def __init__(self) -> None:
        self.dict_balance_name = {'CURRENT ASSETS': ['TOTALCURRENTASSETS'], 'Cash and cash equivalents': ['CASHANDCASHEQUIVALENTS'], 'Short Term Financial Investments': ['SHORTTERMINVESTMENTS'], 'Short term receivables': ['TRADEANDOTHERRECEIVABLES,CURRENT'], 'Total Inventories': ['INVENTORIES'], 'Total Other Current Assets': ['OTHERCURRENTASSETS'], 'TOTAL NON-CURRENT ASSETS': ['TOTALNON-CURRENTASSETS'], 'Fixed assets': ['NETPROPERTY,PLANTANDEQUIPMENT'], 'Total Other non-current assets': ['NETINTANGIBLEASSETS'], 'TOTAL ASSETS': ['TOTALASSETS'], 'LIABILITIES': ['TOTALLIABILITIES'], 'Current liabilities': ['TOTALCURRENTLIABILITIES'], 'Non-current liabilities': ['TOTALNON-CURRENTLIABILITIES'], "TOTAL OWNER'S EQUITY": ['TOTALEQUITY'], "Owner's equity": ['TOTALEQUITYATTRIBUTABLETOPARENTSTOCKHOLDERS', 'EQUITYATTRIBUTABLETOPARENTSTOCKHOLDERS'], 'Other sources and funds_header': ['RESERVES/ACCUMULATEDCOMPREHENSIVEINCOME/LOSSES'], 'Non-controlling interests': ['NON-CONTROLLING/MINORITYINTERESTSINEQUITY'], "TOTAL LIABILITIES AND OWNER'S EQUITY": ['TOTALLIABILITIES', 'TOTALEQUITY']}
        self.list_balance_keys = list(self.dict_balance_name.keys())
        self.list_balance_name = ['CASHANDCASHEQUIVALENTS', 'EQUITYATTRIBUTABLETOPARENTSTOCKHOLDERS', 'INVENTORIES', 'NETINTANGIBLEASSETS', 'NETPROPERTY,PLANTANDEQUIPMENT', 'NON-CONTROLLING/MINORITYINTERESTSINEQUITY', 'OTHERCURRENTASSETS', 'RESERVES/ACCUMULATEDCOMPREHENSIVEINCOME/LOSSES', 'SHORTTERMINVESTMENTS', 'TOTALASSETS', 'TOTALCURRENTASSETS', 'TOTALCURRENTLIABILITIES', 'TOTALEQUITY', 'TOTALEQUITYATTRIBUTABLETOPARENTSTOCKHOLDERS', 'TOTALLIABILITIES', 'TOTALNON-CURRENTASSETS', 'TOTALNON-CURRENTLIABILITIES', 'TRADEANDOTHERRECEIVABLES,CURRENT']
        self.b_month = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

        self.dict_income_name = {'Net sales': 'TOTALREVENUE', 'Cost of sales': 'COSTOFREVENUE', 'Gross profit': 'GROSSPROFIT', 'Selling, general and administrative expenses': 'OPERATINGINCOME/EXPENSES', 'Net operating profit': 'TOTALOPERATINGPROFIT/LOSS', 'Other profit': 'NON-OPERATINGINCOME/EXPENSE,TOTAL', 'Total accounting profit before tax': 'PRETAXINCOME', 'Income taxes': 'PROVISIONFORINCOMETAX', 'Profit after tax': 'NETINCOMEAFTEREXTRAORDINARYITEMSANDDISCONTINUEDOPERATIONS', 'Net profit after tax of the parent': 'NETINCOMEAFTERNON-CONTROLLING/MINORITYINTERESTS', 'Benefits of minority shareholders': 'NON-CONTROLLING/MINORITYINTERESTS', 'Volume': 'BASICWASO'}
        self.list_income_keys = list(self.dict_income_name.keys())
        self.list_income_name = list(self.dict_income_name.values())
        self.list_mul = [1, -1, 1, -1, 1, 1, 1, -1, 1, 1, -1, 1]

    def transform_balance_F0_to_F1(self, data: pd.DataFrame):
        try:
            data = data.fillna(0.0).drop_duplicates(ignore_index=True)
            feature_col = data.columns[0]
            temp_fiscal = data.loc[len(data)-1, feature_col].split("Fiscal year ends in ")[1].split(" |")
            time = temp_fiscal[0]
            try: currency_unit = temp_fiscal[1].replace(" ", "")
            except: currency_unit = ""

            if len(time) > 3 and time[0:3] in self.b_month:
                time = pd.to_datetime(time + ", 2000", format="%b %d, %Y").strftime("%d/%m")
            else:
                return "Không tìm được Time_BCTC", False, currency_unit

            data[feature_col] = data[feature_col].str.replace(" ", "").str.upper()
            data.set_index(feature_col, inplace=True)

            dict_morning = {}
            for name in self.list_balance_name:
                if name in data.index:
                    if len(data.loc[name].shape) == 2:
                        return "Có một feature xuất hiện ở 2 hàng trở lên, dữ liệu khác nhau", False, currency_unit

                    dict_morning[name] = data.loc[name]

            list_row = []
            dict_morning_keys = list(dict_morning.keys())
            data_columns = data.columns
            for name in self.list_balance_keys:
                temp_col = None
                for col in self.dict_balance_name[name]:
                    if col in dict_morning_keys:
                        if temp_col is None:
                            temp_col = dict_morning[col].copy()
                        else:
                            if name == "Owner's equity":
                                if (temp_col != dict_morning[col]).any():
                                    return "2 hàng dữ liệu khác nhau tương ứng Owner's equity", False, currency_unit
                            else:
                                temp_col += dict_morning[col]

                if temp_col is None:
                    temp_col = pd.Series({col:0.0 for col in data_columns})

                list_row.append(temp_col)

            new_data = pd.DataFrame(list_row)
            new_data.index = self.list_balance_keys
            temp_row\
                = new_data.loc["TOTAL NON-CURRENT ASSETS"]\
                - new_data.loc["Fixed assets"]\
                - new_data.loc["Total Other non-current assets"]
            temp_df = pd.DataFrame([temp_row])
            temp_df.index = ["Long-term financial investments"]

            df_rs = pd.concat([new_data.iloc[0:8], temp_df, new_data.iloc[8:]])
            df_rs.columns = [col[:4] for col in df_rs.columns]
            df_1 = df_rs.loc[:, ~df_rs.columns.duplicated()]
            df_2 = df_rs.loc[:, df_rs.columns.duplicated()]
            for col in df_2.columns:
                if (df_2[col] != df_1[col]).all():
                    return "Có 2 cột trùng năm nhưng dữ liệu khác nhau", False, currency_unit
            else:
                df_rs = df_1

            df_rs.rename(columns={data_columns[i]: time+"/"+data_columns[i] for i in range(len(data_columns))}, inplace=True)
            return df_rs, True, currency_unit
        except Exception as ex:
            return ex, False, ""

    def transform_income_F0_to_F1(self, data: pd.DataFrame):
        try:
            data = data.fillna(0.0).drop_duplicates(ignore_index=True)
            if "TTM" in data.columns:
                data.pop("TTM")

            feature_col = data.columns[0]
            temp_fiscal = data.loc[len(data)-1, feature_col].split("Fiscal year ends in ")[1].split(" |")
            time = temp_fiscal[0]
            try: currency_unit = temp_fiscal[1].replace(" ", "")
            except: currency_unit = ""

            if len(time) > 3 and time[0:3] in self.b_month:
                time = pd.to_datetime(time + ", 2000", format="%b %d, %Y").strftime("%d/%m")
            else:
                return "Không tìm được Time_BCTC", False, currency_unit

            data[feature_col] = data[feature_col].str.replace(" ", "").str.upper()
            data.set_index(feature_col, inplace=True)
            data_columns = data.columns
            data.loc["-1"] = {col:0.0 for col in data_columns}

            list_row = []
            for ii in range(len(self.list_income_name)):
                name = self.list_income_name[ii]
                if name in data.index:
                    if len(data.loc[name].shape) == 2:
                        return "Có một feature xuất hiện ở 2 hàng trở lên, dữ liệu khác nhau", False, currency_unit

                    list_row.append(self.list_mul[ii] * data.loc[name])
                else:
                    list_row.append(data.loc["-1"])

            df_rs = pd.DataFrame(list_row)
            df_rs.index = self.list_income_keys
            df_rs.columns = [col[:4] for col in df_rs.columns]
            df_1 = df_rs.loc[:, ~df_rs.columns.duplicated()]
            df_2 = df_rs.loc[:, df_rs.columns.duplicated()]
            for col in df_2.columns:
                if (df_2[col] != df_1[col]).all():
                    return "Có 2 cột trùng năm nhưng dữ liệu khác nhau", False, currency_unit
            else:
                df_rs = df_1
            df_rs.rename(columns={data_columns[i]: time+"/"+data_columns[i] for i in range(len(data_columns))}, inplace=True)
            return df_rs, True, currency_unit
        except Exception as ex:
            return ex, False, ""

# ====================================================================================================

