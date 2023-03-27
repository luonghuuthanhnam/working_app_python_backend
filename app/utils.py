from copy import deepcopy
import pandas as pd
from itertools import product

import datetime

def split_gender_dob(input_json):
    process_json = deepcopy(input_json)
    for idx, each_record in enumerate(process_json):
        dob = None
        gender = None
        if each_record["ngaysinh_nu"] != None:
            dob =  each_record["ngaysinh_nu"]
            gender = "Nữ"
        if each_record["ngaysinh_nam"] != None:
            dob =  each_record["ngaysinh_nam"]
            gender = "Nam"
        process_json[idx]["gioitinh"] = gender
        process_json[idx]["ngaysinh"] = dob
    return process_json

def gender_pie_chart(dataframe_input):
    male_count = sum(dataframe_input["ngaysinh_nam"].value_counts())
    female_count = sum(dataframe_input["ngaysinh_nu"].value_counts())
    output_dict = [
        {
            "type": "Nam",
            "value": male_count
        },
        {
            "type": "Nữ",
            "value": female_count
        }
    ]
    return output_dict

def joining_date_by_gender(length, main_data, mapping_cols_swap):
    result =  main_data.iloc[:int(length)]
    new_result = result.rename(columns=mapping_cols_swap)
    new_result = new_result.reset_index(drop=True)
    union_joining_date_by_gender_df = new_result[["ngayvao_congdoan", "ngaysinh_nu", "ngaysinh_nam"]]
    gender = []
    joining_date = []
    union_joining_date_by_gender_df = union_joining_date_by_gender_df.fillna("")
    for irow in range(len(union_joining_date_by_gender_df)):
        row  = union_joining_date_by_gender_df.iloc[irow]
        if row["ngaysinh_nu"] == "":
            gender.append("nam")
        else:
            gender.append("nu")
        joining_year = row["ngayvao_congdoan"].split("/")[-1]
        joining_date.append(int(joining_year))
    union_joining_date_by_gender_df["gender"] = gender
    union_joining_date_by_gender_df["joining_date"] = joining_date
    final_df = union_joining_date_by_gender_df[["gender", "joining_date"]]
    final_df = pd.DataFrame({'value' : final_df.groupby( ["gender", "joining_date"] ).size()}).reset_index()
    final_df = final_df.sort_values("joining_date")
    df = final_df.copy()
    

    nam_df = df[df['gender']=='nam']
    nam_df = nam_df.rename(columns={'value':'nam_value'})
    nam_df = nam_df.drop(columns=['gender'])

    # create a dataframe for nu
    nu_df = df[df['gender']=='nu']
    nu_df = nu_df.rename(columns={'value':'nu_value'})
    nu_df = nu_df.drop(columns=['gender'])

    # merge nam and nu dataframes on joining_date
    merged_df = pd.merge(nam_df, nu_df, on='joining_date', how='outer')

    # create a new column 'tong_value' as the sum of 'nam_value' and 'nu_value'
    merged_df['tong_value'] = merged_df['nam_value'].fillna(0) + merged_df['nu_value'].fillna(0)

    # create a new dataframe for 'tong'
    tong_df = merged_df[['joining_date', 'tong_value']]
    tong_df = tong_df.rename(columns={'tong_value':'value'})
    tong_df['gender'] = 'tong'

    # append 'tong' dataframe to the original dataframe
    df = df.append(tong_df, ignore_index=True)

    # sort the dataframe by gender, joining_date
    df = df.sort_values('joining_date').reset_index(drop=True)

    # fill missing values with 0
    df['value'] = df['value'].fillna(0)

    # create a new DataFrame with all possible combinations of gender and year
    all_genders = df['gender'].unique()
    min_year = df['joining_date'].min()
    max_year = df['joining_date'].max()
    year_range = range(min_year, max_year+1)
    all_years = pd.DataFrame(list(product(all_genders, year_range)), columns=['gender', 'joining_date'])

    # merge with the original DataFrame to fill missing values with 0
    result = pd.merge(df, all_years, how='right', on=['gender', 'joining_date'])
    result['value'] = result['value'].fillna(0)

    # sort the result by gender and joining_date
    result.sort_values(['gender', 'joining_date'], inplace=True)

    # reset the index
    result.reset_index(drop=True, inplace=True)
    result = result.sort_values('joining_date').reset_index(drop=True)
    result = result[result["gender"]!= "tong"]
    result["gender"] = result["gender"].replace("nam", "Nam")
    result["gender"] = result["gender"].replace("nu", "Nữ")
    return result.to_json(orient="records")


def cal_age(length, main_data, mapping_cols_swap):
    result =  main_data.iloc[:int(length)]
    new_result = result.rename(columns=mapping_cols_swap)
    new_result = new_result.reset_index(drop=True)
    date_gender_df = new_result[["ngaysinh_nu", "ngaysinh_nam"]]
    gender = []
    born = []
    age = []
    date_gender_df = date_gender_df.fillna("")
    for irow in range(len(date_gender_df)):
        row  = date_gender_df.iloc[irow]
        byear = ''
        if row["ngaysinh_nam"] != "":
            gender.append("nam")
            byear = int(row["ngaysinh_nam"].split("/")[-1])
            born.append(byear)
        if row["ngaysinh_nu"] != "":
            gender.append("nu")
            byear = int(row["ngaysinh_nu"].split("/")[-1])
            born.append(byear)
        age.append(str(int(datetime.datetime.now().year) - byear))
    date_gender_df["gender"] = gender
    date_gender_df["born"] = born
    date_gender_df["age"] = age
    date_gender_df = date_gender_df.drop(columns=["ngaysinh_nam", "ngaysinh_nu", "gender"])
    result_df = date_gender_df.drop(columns=["born"])
    result_df = result_df.value_counts("age").reset_index()
    result_df.columns = ['age', 'value']
    result_df = result_df.sort_values("age")
    return result_df.to_json(orient="records")

def province_distribution(length, main_data, mapping_cols_swap, top = -1):
    result =  main_data.iloc[:int(length)]
    new_result = result.rename(columns=mapping_cols_swap)
    new_result = new_result.reset_index(drop=True)
    result_df = new_result.value_counts("tinh").reset_index()
    result_df.columns = ['province', 'value']
    result_df = result_df.sort_values("value", ascending = False)
    if top != -1:
        result_df = result_df[:top]
    return result_df.to_json(orient="records")
