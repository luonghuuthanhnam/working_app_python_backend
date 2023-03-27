import sys
import os
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel
import json
from app.utils import split_gender_dob, gender_pie_chart, joining_date_by_gender, cal_age, province_distribution
from fastapi.middleware.cors import CORSMiddleware
import app.login_signup_handler as login_signup_handler
import app.event_db_handler as event_db_handler

origins = [
    "http://localhost",
    "http://localhost:8000",
    "http://localhost:3000",
    "http://localhost:5000",
]

employee_raw_data = pd.read_excel("app/database/DanhSachThongTinDoanVien.xlsx")
main_data = employee_raw_data.iloc[5:]

mapping_cols = {
    "stt": "Unnamed: 0",
    "maso_doanvien": "Unnamed: 1",
    "hovaten": "Unnamed: 2",
    "ngaysinh_nam": "Unnamed: 3",
    "ngaysinh_nu": "Unnamed: 4",
    "tinh": "Unnamed: 5",
    "matinh": "Unnamed: 6",
    "cmnd": "Unnamed: 7",
    "huongluong_ngansach": "Unnamed: 8",
    "huongluong_ngoaingansach": "Unnamed: 9",
    "khongchuyentrach": "Unnamed: 10",
    "chutich": "Unnamed: 11",
    "phochutich": "Unnamed: 12",
    "uvbch": "Unnamed: 13",
    "totruong": "Unnamed: 14",
    "topho": "Unnamed: 15",
    "cb_cdcs": "Unnamed: 16",
    "vuvbkt": "Unnamed: 17",
    "ngayvao_congdoan": "Unnamed: 18",
    "Unnamed_19": "Unnamed: 19",
    "Unnamed_20": "Unnamed: 20",
    "Unnamed_21": "Unnamed: 21",
    "Unnamed_22": "Unnamed: 22",
    "nguyenquan_tinh": "Unnamed: 23",
    "nguyenquan_matinh": "Unnamed: 24",
}
mapping_cols_swap = {v: k for k, v in mapping_cols.items()}
class QueryEmployee(BaseModel):
    length: int

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    # allow_origins=origins,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
@app.get("/")
def read_root():
    return {"Data_App": "Success"}

@app.post("/imployee/query")
async def employee_query(data: QueryEmployee):
    length = data.length
    if int(length) == -1:
        length = len(main_data)

    result =  main_data.iloc[:int(length)]
    new_result = result.rename(columns=mapping_cols_swap)
    new_result = new_result.reset_index(drop=True)
    new_result_js = json.loads(new_result.to_json(orient='records'))
    new_result_js = split_gender_dob(new_result_js)

    pie_dict = gender_pie_chart(new_result)

    joining_by_gender_json = joining_date_by_gender(length, main_data, mapping_cols_swap)

    age_distribution = cal_age(length, main_data, mapping_cols_swap)
    provice_dis = province_distribution(length, main_data, mapping_cols_swap, top = 15)
    output_dict = {
        "main_data": new_result_js,
        "pie_chart": pie_dict,
        "joining_by_gender": joining_by_gender_json,
        "age_distribution": age_distribution,
        "province_distribution": provice_dis
    }
    return output_dict

class LoginData(BaseModel):
    email: str
    password: str

loginHandler = login_signup_handler.LoginHandler()


@app.post("/WorkingApp/Auth/Login")
async def working_app_login(data: LoginData):
    email = data.email
    password = data.password
    user_id = loginHandler.login_request(user_name=email, password=password)
    res_json = {
        "user_id": user_id
    }
    return res_json


class QueryEventList(BaseModel):
    userId: str

event_db_excel_path = "app/database/event_db.xlsx"
eventDBHandler = event_db_handler.EventDBHandler(event_db_excel_path)
@app.post("/WorkingApp/Event/EventData")
async def query_event_list(data: QueryEventList):
    print(data)
    userId = data.userId
    if userId == None:
        return None
    eventDBHandler
    event_data_dict = eventDBHandler.query_evet_by_userId(userId=userId)
    return event_data_dict


class create_event_data(BaseModel):
    event_data: dict
    user_id: str

eventHandler =  event_db_handler.EventHandler()
@app.post("/event/create")
async def create_event_handler(data: create_event_data):
    try:
        print(data)
        eventHandler.create_event(data.event_data, user_id=data.user_id)
        return {"message": "Create event success"}
    except Exception as e:
        return {"message": "Create event failed"}
