from copy import deepcopy
import pandas as pd
from itertools import product

import datetime


def gender_pie_chart(dataframe_input):
    male_count = 0
    female_count = 0
    if "Nam" in dataframe_input["gioitinh"].value_counts().keys():
        male_count = dataframe_input["gioitinh"].value_counts()["Nam"]
    if "Nữ" in dataframe_input["gioitinh"].value_counts().keys():
        female_count = dataframe_input["gioitinh"].value_counts()["Nữ"]
    output_dict = [
        {
            "type": "Nam",
            "value": int(male_count)
        },
        {
            "type": "Nữ",
            "value": int(female_count)
        }
    ]
    return output_dict

def joining_date_by_gender(length, main_data):
    result =  main_data.iloc[:int(length)]
    new_result = result.reset_index(drop=True)
    joining_years = [int(each_date.split("/")[-1]) for each_date in new_result["ngayvao_congdoan"]]
    new_result["namvao_congdoan"] = joining_years
    final_df = pd.DataFrame({'value' : new_result.groupby( ["gioitinh", "namvao_congdoan"] ).size()}).reset_index()
    final_df = final_df.sort_values("namvao_congdoan")
    df = final_df.copy()
    

    nam_df = df[df['gioitinh']=='nam']
    nam_df = nam_df.rename(columns={'value':'nam_value'})
    nam_df = nam_df.drop(columns=['gioitinh'])

    # create a dataframe for nu
    nu_df = df[df['gioitinh']=='nu']
    nu_df = nu_df.rename(columns={'value':'nu_value'})
    nu_df = nu_df.drop(columns=['gioitinh'])

    # merge nam and nu dataframes on joining_date
    merged_df = pd.merge(nam_df, nu_df, on='namvao_congdoan', how='outer')

    # create a new column 'tong_value' as the sum of 'nam_value' and 'nu_value'
    merged_df['tong_value'] = merged_df['nam_value'].fillna(0) + merged_df['nu_value'].fillna(0)

    # create a new dataframe for 'tong'
    tong_df = merged_df[['namvao_congdoan', 'tong_value']]
    tong_df = tong_df.rename(columns={'tong_value':'value'})
    tong_df['gioitinh'] = 'tong'

    # append 'tong' dataframe to the original dataframe
    df = df.append(tong_df, ignore_index=True)

    # sort the dataframe by gender, joining_date
    df = df.sort_values('namvao_congdoan').reset_index(drop=True)

    # fill missing values with 0
    df['value'] = df['value'].fillna(0)

    # create a new DataFrame with all possible combinations of gender and year
    all_genders = df['gioitinh'].unique()
    min_year = df['namvao_congdoan'].min()
    max_year = df['namvao_congdoan'].max()
    try:

        year_range = range(min_year, max_year+1)
    
    except:
        print("Error: ", min_year, max_year)

    all_years = pd.DataFrame(list(product(all_genders, year_range)), columns=['gioitinh', 'namvao_congdoan'])

    # merge with the original DataFrame to fill missing values with 0
    result = pd.merge(df, all_years, how='right', on=['gioitinh', 'namvao_congdoan'])
    result['value'] = result['value'].fillna(0)

    # sort the result by gender and joining_date
    result.sort_values(['gioitinh', 'namvao_congdoan'], inplace=True)

    # reset the index
    result.reset_index(drop=True, inplace=True)
    result = result.sort_values('namvao_congdoan').reset_index(drop=True)
    result = result[result["gioitinh"]!= "tong"]
    result["gioitinh"] = result["gioitinh"].replace("nam", "Nam")
    result["gioitinh"] = result["gioitinh"].replace("nu", "Nữ")
    result.rename(columns={"gioitinh": "gender", "namvao_congdoan": "joining_date"}, inplace=True)
    return result.to_json(orient="records")

def cal_age(length, main_data):
    result =  main_data.iloc[:int(length)]
    new_result = result.reset_index(drop=True)
    birth_year = [int(each_date.split("/")[-1]) for each_date in new_result["ngaysinh"]]
    cur_year = int(datetime.datetime.now().year)
    age = [str(cur_year - each_birth_year) for each_birth_year in birth_year]
    new_result["namsinh"] = birth_year
    new_result["age"] = age
    result_df = new_result.value_counts("age").reset_index()
    result_df.columns = ['age', 'value']
    result_df = result_df.sort_values("age")
    return result_df.to_json(orient="records")

def province_distribution(length, main_data, top = -1):
    result =  main_data.iloc[:int(length)]
    new_result = result.reset_index(drop=True)
    result_df = new_result.value_counts("tinh").reset_index()
    result_df.columns = ['tinh', 'value']
    result_df.rename(columns={"tinh": "province"}, inplace=True)
    result_df = result_df.sort_values("value", ascending = False)
    if top != -1:
        result_df = result_df[:top]
    return result_df.to_json(orient="records")

def working_status(length, main_data):
    result =  main_data.iloc[:int(length)]
    new_result = result.reset_index(drop=True)
    new_result["status"] = new_result["status"].replace("0", "Đã nghỉ")
    new_result["status"] = new_result["status"].replace("1", "Đang làm")
    result_status = new_result.value_counts("status").reset_index()
    result_status.columns = ['type', 'value']
    # print(result_status)
    result = result_status.to_dict(orient="records")
    print(result)
    return result

def sunburst_total_department(length, main_data):
    grouped_emp_df = main_data.groupby("group_name").apply(lambda x: x.reset_index(drop=True))
    unique_group = grouped_emp_df["group_name"].unique() 
    children = []
    for group_name in unique_group:
        cur_group_df = grouped_emp_df.loc[group_name]
        cur_group_df = cur_group_df[["employee_id", "gioitinh"]]
        cur_group_gender = cur_group_df.value_counts("gioitinh")
        total_value = sum(cur_group_gender)
        male_count = 0
        female_count = 0
        if "Nam" in cur_group_gender:
            male_count = cur_group_gender["Nam"]
        if "Nữ" in cur_group_gender:
            female_count = cur_group_gender["Nữ"]
        group_restruct_data = {
            "name": group_name,
            "value": total_value,
            "children": [
                {
                "name": "Nam",
                "value": int(male_count),
                },
                {
                "name": "Nữ",
                "value": int(female_count),
                },

            ]
        }
        children.append(group_restruct_data)

    total_restructed_data = {
        "name": "Đơn vị",
        "children": children
    }
    return total_restructed_data

from login_signup_handler import LoginHandler
loginHandler = LoginHandler()
class GroupData():
    def __init__(self, group_data_excel_path= "database/group_data.xlsx") -> None:
        self.group_data_excel_path = group_data_excel_path
        self.group_data_df = pd.read_excel(self.group_data_excel_path)
    
    def reload(self):
        self.group_data_df = pd.read_excel(self.group_data_excel_path)

    def get_group_name(self, group_id):
        group_name = self.group_data_df[self.group_data_df["group_id"] == group_id]["group_name"].values[0]
        return group_name

    def get_group_id(self, group_name):
        group_id = self.group_data_df[self.group_data_df["group_name"] == group_name]["group_id"].values[0]
        return group_id

    def get_group_id_by_user_id(self, user_id):
        user_info = loginHandler.get_user_info_by_id(user_id)
        if user_info != None:
            group_id = user_info["group_id"]
            return group_id
        else:
            return None
    
    def get_all_group_id_name(self):
        return self.group_data_df[["group_id", "group_name"]].to_dict(orient="records")
