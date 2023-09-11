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

class IrBank:
    def __init__(self) -> None:
        self.df_ft_ic = pd.read_excel(ROOT_PATH + "\\Transform\\Base\\JP_Lib.xlsx", sheet_name="JP_Income")
        self.df_ft_ba = pd.read_excel(ROOT_PATH + "\\Transform\\Base\\JP_Lib.xlsx", sheet_name="JP_Balance")
        self.df_ft_ic = self.convertFeature("ic")
        self.df_ft_ba = self.convertFeature("ba")

        self.LST_SUM_FEATURE = [
                                ['Short Term Financial Investments',
                                'Short Term Financial Investments_1'],
                                ['Short term receivables',
                                'STR_1',
                                'STR_2',
                                'STR_3',
                                'STR_4',
                                'STR_5',
                                'STR_6',
                                'STR_7',
                                'STR_8',
                                'STR_9',
                                'STR_10',
                                'STR_11',
                                'STR_12',
                                'STR_13',
                                'STR_14',
                                'STR_15',
                                'STR_16',
                                'STR_17',
                                'STR_18',
                                'STR_19'],
                                ['Total Inventories',
                                'TotalInventories_1',
                                'TotalInventories_2',
                                'TotalInventories_3',
                                'TotalInventories_4',
                                'TotalInventories_5',
                                'TotalInventories_6',
                                'TotalInventories_7',
                                'TotalInventories_8',
                                'TotalInventories_9',
                                'TotalInventories_10',
                                'TotalInventories_11',
                                'TotalInventories_12',
                                'TotalInventories_13',
                                'TotalInventories_14',
                                'TotalInventories_15',
                                'TotalInventories_16',
                                'TotalInventories_17',
                                'TotalInventories_18',
                                'TotalInventories_19'],
                                ['Total Other Current Assets',
                                'Total Other Current Assets_1',
                                'Total Other Current Assets_2',
                                'Total Other Current Assets_3',
                                'Total Other Current Assets_4'],
                                ['Fixed assets',
                                'Fixed assets_1',
                                'Fixed assets_2']
                                ]

        self.CHARACTER = '-------'
        self.LIST_FEATURE_INCOME = ['Net sales',
                                    'Cost of sales',
                                    'Gross profit',
                                    'Selling, general and administrative expenses',
                                    'Net operating profit',
                                    'Other profit',
                                    'Total accounting profit before tax',
                                    'Income taxes',
                                    'Profit after tax',
                                    'Net profit after tax of the parent',
                                    'Benefits of minority shareholders',
                                    ]

    def convertFeature(self, type_):
        '''
        :param type_: "ic" (income) hoặc "ba" (balance)
        '''

        if type_ == "ic":
            df_feature = self.df_ft_ic
        elif type_ == "ba":
            df_feature = self.df_ft_ba
        else: raise

        df_split =  df_feature['Japan'].str.split(',', expand = True)
        df_feature['Japan'] = df_feature['Japan'].str.replace(' ','')
        df_feature[np.arange(len(df_split.columns))] = df_feature['Japan'].str.split(',', expand = True)
        try:
            df_feature.drop(columns = ['Japan', 'Ghi chú'], inplace = True)
        except:
            df_feature.drop(columns = ['Japan'], inplace = True)
        return df_feature.reset_index(drop = True)

    def fillNanColName(self, df):
        """
        Fill nan in column name

        Parameters
        ----------
        df : DataFrame
            DataFrame need to fill nan in column name

        Returns
        -------
        df : DataFrame
            DataFrame after fill nan in column name
        """

        indices = df[df.apply(lambda row: row.count == 1, axis=1)].index
        index_fill = df[pd.isna(df.iloc[:,0])].index
        for f_id in index_fill:
            for j in range(len(indices)-1):
                if (indices[j] < f_id) and (indices[j+1] > f_id):
                    df.iloc[f_id, 0] = df.iloc[indices[j], 0]
                    break
        return df

    def cutData(self, x):
        """
        Cut data in column Feature

        Parameters
        ----------
        x : str
            Data need to cut

        Returns
        -------
        result : str
            Data after cut
        """

        if pd.isna(x): return x
        result = re.sub(r'\（.*?\）', ' ', x)
        return result.replace(' ', '')

    def renameDateColumn(self, x):
        """
        Rename column

        Parameters
        ----------
        x : str
            Column name need to rename

        Returns
        -------
        x : str
            Column name after rename

        """
        if x == 'Feature': return x
        x = x.replace('.1', '')
        try:
            return (pd.to_datetime(x)).strftime('%-d/%-m/%Y')
        except:
            return x

    def dropColumnDuplicateF1(self, df):
        """
        Drop column duplicate in DataFrame

        Parameters
        ----------
        df : DataFrame
            DataFrame need to drop column duplicate

        Returns
        -------
        df : DataFrame
            DataFrame after drop column duplicate
        """

        df = df.T.reset_index().drop_duplicates()
        df.index = df['index']
        df.drop(columns = ['index'], inplace = True)
        df = df.T
        return df

    def transform_F0_to_F1(self, df: pd.DataFrame, type_):
        '''
        :param type_: "ic" (income) hoặc "ba" (balance)
        '''

        df = self.fillNanColName(df)

        # Đổi đơn vị
        df = df.replace('－', np.nan).replace("（百万円）", ",000,000").replace("（千円）", ",000").replace("（円）", "")
        df.iloc[1:, 1:] += df.loc[0]

        # Xoá các trường hàng rỗng data
        df = df.dropna(thresh=2).reset_index(drop = True)

        # Xử lý feature
        df.iloc[:, 0] = df.iloc[:, 0].str.replace(' ', '')
        df.iloc[:, 0] = df.iloc[:, 0].apply(lambda x : self.cutData(x))

        # Đổi tên column
        df.columns = df.columns.str.replace(' ', '').str.replace('自', '').str.replace('年', '/').str.replace('月', '/').str.replace('日', '') # change time

        if type_ == "ic":
            df.columns = ['Feature'] + [i[1] for i in df.columns.str.split('至')[1:]] # rename column income
        elif type_ == "ba":
            df.columns = ['Feature'] + [i[0] for i in df.columns.str.split('至')[1:]] # rename column balance
        else: raise

        df = df.rename(columns=lambda x: self.renameDateColumn(x))

        #Replace và drop cột feature nan
        df['Feature'].replace('', np.nan, inplace = True)
        df.dropna(subset = 'Feature', inplace = True)
        df = df.reset_index(drop = True)

        df = self.dropColumnDuplicateF1(df)
        # if len(df.columns) - len(set(df.columns)):
        #     print('-----------------_____---------------', symbol)
        #     print('_____________________________________')
        return df

    def transform_F1_to_F2(self, df: pd.DataFrame, type_):
        '''
        :param type_: "ic" (income) hoặc "ba" (balance)
        '''

        if type_ == "ic":
            df_feature = self.df_ft_ic
        elif type_ == "ba":
            df_feature = self.df_ft_ba
        else: raise

        lst_feature = list(df_feature['English'])

        # Xử lý F1 để đưa về dạng F2
        # merge để lấy các feature trong F1
        for id_col in range(20):
            try:
                df_feature_temp = df_feature[['English', id_col]].rename(columns = {id_col:'Feature'})
                df_merge_temp = pd.merge(df_feature_temp, df, on = 'Feature')
                if id_col == 0:
                    df_f2 = df_merge_temp
                else:
                    df_f2 = pd.concat([df_f2, df_merge_temp]).reset_index(drop = True)
            except:
                break
        lst_data_find = list(df_f2['English'])
        result = list(set(lst_feature).difference(lst_data_find))
        for i in result:
            df_f2.loc[sum(df_f2.index)] = [i] + list(np.full(len(df_f2.columns)-1, -1))
        df_f2 = df_f2.sort_values(by='English', key=lambda x: x.map(dict(zip(lst_feature, range(len(lst_feature)))))).reset_index(drop = True) # sort by feature
        return df_f2

    def readF2(self, df):
        """
        Read data from Financial_F2, replace nan and -1 to 0, convert data to numeric

        Parameters
        ----------
        path : str
            Path of file

        Returns
        -------
        df : DataFrame
            DataFrame after read
        """

        df = df.fillna(0).replace('-1', '0').replace(-1, 0)
        df.iloc[:, 2:] = df.iloc[:, 2:].applymap(lambda x: pd.to_numeric(str(x).replace(',', ''), errors='coerce'))
        if len(df.columns) == 3:
            df[f'{df.columns[2]}_x'] = df[df.columns[2]]
        return df

    def sumFeatureBalance(self, df):
        """
        Sum feature in BalanceSheet by list_sum_feature

        Parameters
        ----------
        df : DataFrame
            DataFrame of BalanceSheet

        Returns
        -------
        df : DataFrame
            DataFrame after sum
        """

        for i in range(len(self.LST_SUM_FEATURE)):
            df_short_temp = df[df['English'].isin(self.LST_SUM_FEATURE[i])]
            first_id = df_short_temp.index[0]
            list_data = [df_short_temp.iloc[0, 0], ''] + list(df_short_temp.iloc[:, 2:].sum())
            df.drop(df_short_temp.index, inplace = True)
            df.loc[first_id] = list_data
        return df.sort_index()

    def chooseColumnData(self, df_temp):
        """
        Choose data in column use for mergeDF

        Parameters
        ----------
        df_temp : DataFrame
            DataFrame of column

        Returns
        -------
        data : int
            Data after choose
        """
        lst_data = np.unique(df_temp.to_numpy())
        if len(lst_data) == 1:
            return lst_data[0]
        if (len(lst_data) == 2) and (0 in lst_data):
            for i in lst_data:
                if i != 0: return i
        return self.CHARACTER

    def mergeDF(self, df_temp):
        """
        Merge data in column when use groupby

        Parameters
        ----------
        df_temp : DataFrame
            DataFrame of column

        Returns
        -------
        df_temp : DataFrame
            DataFrame after merge
        """

        if len(df_temp.index) == 1: return df_temp
        df_temp_2 = df_temp.copy()
        df_temp.iloc[:, 2:] = df_temp.iloc[:, 2:].apply(lambda y: self.chooseColumnData(y), axis = 0)

        # drop duplicate bug character
        if self.CHARACTER in df_temp.values:
            df_temp_2.drop_duplicates(inplace = True)
            return df_temp_2
        df_temp['Feature'] = np.nan

        # drop duplicate
        df_temp.drop_duplicates(inplace = True)
        return df_temp

    def makeSomeFeature(self, df, type_):
        """
        Make some feature in IncomeStatement and BalanceSheet
        other profit = financial income - financial expenses + other income - other expenses
        SG&A = SG&A + cost other - benefit other
        Long-term financial investments = total non-current assets - fixed assets - total other non-current assets

        Parameters
        ----------
        df : DataFrame
            DataFrame of IncomeStatement or BalanceSheet
        PATH_DATA : str
            Type of data (IncomeStatement or BalanceSheet)

        Returns
        -------
        df : DataFrame
            DataFrame after make
        """
        df.index = df['Feature']
        df.drop(columns = ['Feature'], inplace = True)
        df = df.T
        if type_ == "ic":
            df['Other profit'] = df['Financial income'] - df['Financial expenses'] + df['Other income'] - df['Other expenses']
            df['Selling, general and administrative expenses'] = df['Selling, general and administrative expenses'] + df['Cost_other'] - df['Benefit_other']
            df.drop(columns = ['Financial income', 'Financial expenses', 'Other income', 'Other expenses', 'Cost_other', 'Benefit_other'], inplace = True)
        elif type_ == "ba":
            df['Long-term financial investments'] = df['TOTAL NON-CURRENT ASSETS'] - df['Fixed assets']  - df['Total Other non-current assets']
        else: raise

        df = df.T
        df.reset_index(inplace = True)
        return df

    def sumFeatureHave_2(self, df_temp, df):
        """
        Sum feature have _2 in IncomeStatement and BalanceSheet

        Parameters
        ----------
        df_temp : DataFrame
            DataFrame of IncomeStatement or BalanceSheet
        df : DataFrame
            DataFrame of IncomeStatement or BalanceSheet

        Returns
        -------
        df_temp : DataFrame
            DataFrame after sum
        """

        # sum feature have _2
        if '_2' in df_temp.iloc[0, 0]:
            df_temp.iloc[:, 1:] = np.sum(df_temp.iloc[:, 1:])
            df_temp.drop_duplicates(inplace = True)
            return df_temp

        # Ngoại lệ
        if 'Net profit after tax of the parent' in df_temp.iloc[0, 0]:
            if len(df_temp) > 1:
                df_benefit = df[df['Feature'] == 'Benefits of minority shareholders'] # Tim dòng Benefits of minority shareholders
                for col in range(1, len(df.columns)):
                    lst_data_col = list(df_temp.iloc[:, col].unique()) #chọn các trường có dữ liệu trongo
                    lst_data_col.remove(0) #xoa 0
                    if len(lst_data_col) == 1: # chọn chính nó
                        df_temp.iloc[:, col] = lst_data_col[0]
                    if len(lst_data_col) == 2: # chọn theo điều kiện
                        if df_benefit.iloc[0, col] >= 0:
                            df_temp.iloc[:, col] = min(lst_data_col)
                        else:
                            df_temp.iloc[:, col] = max(lst_data_col)
                df_temp.drop_duplicates(inplace = True) #Lọc dòng trùng
                return df_temp
        return df_temp

    def chooseFeature(self, df, feature):
        """
        Choose feature in IncomeStatement and BalanceSheet

        Parameters
        ----------
        df : DataFrame
            DataFrame of IncomeStatement or BalanceSheet
        feature : str
            Feature need choose

        Returns
        -------
        df : DataFrame
            DataFrame after choose
        """

        # Chọn theo thứ tự ưu tiên, nếu không có thì chọn theo thứ tự ưu tiên tiếp theo
        # mặc đinhj, -> _1, -> _2
        df0 = df[df['Feature'] == feature]
        id = df0.index[0]
        lst_data_1 = df[df['Feature'] == f'{feature}_1'].iloc[0, 1:]
        lst_data_2 = df[df['Feature'] == f'{feature}_2'].iloc[0, 1:]
        lst_data = list(df0.iloc[0, 1:])

        for i in range(len(lst_data)):
            if lst_data[i] == 0:
                if lst_data_1[i] != 0:
                    lst_data[i] = lst_data_1[i]
                else:
                    lst_data[i] = lst_data_2[i]
        df.loc[id, df.columns[1:]] = lst_data
        return df

    def dropFeature(self, df):
        """
        Drop feature in IncomeStatement and BalanceSheet

        Parameters
        ----------
        df : DataFrame
            DataFrame of IncomeStatement or BalanceSheet

        Returns
        -------
        df : DataFrame
            DataFrame after drop
        """

        df.index = df['Feature']
        df.drop(columns = ['Feature'], inplace = True)
        df = df.T

        # drop feature
        try:
            df.drop(columns = ['Net sales_1', 'Net sales_2',
                            'Cost of sales_1', 'Cost of sales_2',
                            'Gross profit_1', 'Gross profit_2',
                            'Income taxes_1', 'Income taxes_2',
                            'Selling, general and administrative expenses_1', 'Selling, general and administrative expenses_2',
                            'Income taxes_1', 'Income taxes_2',
                            'Profit after tax_1','Profit after tax_2',
                            'Cost_other_1','Cost_other_2',
                            'Benefit_other_1','Benefit_other_2'], inplace = True)
        except:
            df.drop(columns = ['Total Other non-current assets_1', 'Total Other non-current assets_2',
                               'Cash and cash equivalents_1','Cash and cash equivalents_2'],  inplace = True)
        df = df.T
        df.reset_index(inplace = True)
        return df

    def fitProfitAfterTax(self, df):
        """
        Fit feature "Profit after tax"

        Parameters
        ----------
        df : DataFrame
            DataFrame of IncomeStatement or BalanceSheet

        Returns
        -------
        df : DataFrame
            DataFrame after fit
        """

        #theo điều kiện chị Trang đưa ra
        df_temp = df.copy()
        df_temp.index = df_temp['Feature']
        df_temp = df_temp.drop(columns = ['Feature'])
        df_temp = df_temp.T
        df_1 = df_temp[df_temp['Profit after tax'] == 0]
        df_2 = df_temp[df_temp['Net profit after tax of the parent'] == 0]
        df_temp['Profit after tax'][df_1.index] = df_1['Net profit after tax of the parent'] + df_1['Benefits of minority shareholders']
        df_temp['Net profit after tax of the parent'][df_2.index] = df_2['Profit after tax'] - df_2['Benefits of minority shareholders']
        df_temp = df_temp.T.reset_index()
        return df_temp

    def dropColumnDuplicateF3(self,df):
        """
        Drop column duplicate in F3
        Có một vài cột bị trùng lặp, xoá đi
        Parameters
        ----------
        df : DataFrame
            DataFrame of F3

        Returns
        -------
        df : DataFrame
            DataFrame after drop
        """
        df.columns = df.columns.str.replace('_x', '').str.replace('_y', '')
        df.index = df['Feature']
        df.drop(columns = ['Feature'], inplace = True)
        df = df.T.reset_index()
        df['index'] = df['index'].str.replace('.1', '', regex = False)
        df['index'] = pd.to_datetime(df['index'])
        df['index'] = df['index'].dt.strftime('%d/%m/%Y')
        df.index = df['index']
        df.drop(columns = ['index'], inplace = True)
        df = df.T
        df = self.dropColumnDuplicateF1(df)
        df = df.reset_index()
        return df

    def transform_F2_to_F3(self, df: pd.DataFrame, type_):
        '''
        :param type_: "ic" (income) hoặc "ba" (balance)
        '''

        if type_ == "ic":
            df_feature = self.df_ft_ic
        elif type_ == "ba":
            df_feature = self.df_ft_ba
        else: raise

        # read feature
        lst_feature = list(df_feature['English'])
        if type_ == "ic":
            lst_feature = self.LIST_FEATURE_INCOME

        #read data F2
        df = self.readF2(df)

        if type_ == "ba":
            df = self.sumFeatureBalance(df)

        # xử lý merge, sắp xếp theo feature và đổi tên cột
        df = df.groupby('English', group_keys=False).apply(lambda x: self.mergeDF(x))
        df = df.sort_values(by='English', key=lambda x: x.map(dict(zip(lst_feature, range(len(lst_feature)))))).reset_index(drop = True)
        df.drop(columns = ['Feature'], inplace = True)
        df = df.rename(columns={'English': 'Feature'})
        df.sort_index(inplace = True)

        #Xử lý các trường theo điều kiện khác nhau(Theo chị Trang)
        if type_ == "ic":
            df = df.groupby('Feature', group_keys=False).apply(lambda x: self.sumFeatureHave_2(x, df))
            for feature in ['Net sales', 'Cost of sales', 'Gross profit', 'Income taxes', 'Selling, general and administrative expenses','Profit after tax','Cost_other','Benefit_other']:
                df = self.chooseFeature(df, feature)
            df = self.makeSomeFeature(df, type_)
            df = self.dropFeature(df)
            df = self.fitProfitAfterTax(df)

        #Xử lý các trường theo điều kiện khác nhau(Theo chị Trang)
        elif type_ == "ba":
            df = df.groupby('Feature', group_keys=False).apply(lambda x: self.sumFeatureHave_2(x, df))
            for feature in ['Total Other non-current assets','Cash and cash equivalents']:
                df = self.chooseFeature(df, feature)
            df = self.makeSomeFeature(df, type_)
            df = self.dropFeature(df)

        # Sắp xếp theo feature và đổi tên cột, lưu file
        df = df.sort_values(by='Feature', key=lambda x: x.map(dict(zip(self.LIST_FEATURE_INCOME, range(len(self.LIST_FEATURE_INCOME)))))).reset_index(drop = True)
        df = self.dropColumnDuplicateF3(df)

        return df

    def check_F3(self, df):
        if (len(df.columns) - len(set(df.columns))) == 0 and  len(df[df.duplicated(subset='Feature')]) == 0: # Lưu file đúng
            return True
        else:
            return False
