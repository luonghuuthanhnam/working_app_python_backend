import sys
import os
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel
import json
import ssl
import uvicorn
from utils import (
    # split_gender_dob,
    gender_pie_chart,
    joining_date_by_gender,
    cal_age,
    province_distribution,
    GroupData
)
from fastapi.middleware.cors import CORSMiddleware
import login_signup_handler as login_signup_handler
import event_db_handler as event_db_handler

# ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
# ssl_context.load_cert_chain("server.crt", "server.key", "server.pem")
# ssl_context.load_cert_chain()
origins = [
    "http://localhost",
    "http://localhost:8000",
    "http://localhost:3000",
    "http://localhost:5000",
]

# employee_raw_data = pd.read_excel("database/DanhSachThongTinDoanVien.xlsx")
main_data = pd.read_excel("database/final_employee_data.xlsx", dtype=str)
groupData = GroupData("database/group_data.xlsx")
# main_data = employee_raw_data.iloc[5:]

# mapping_cols = {
#     "stt": "Unnamed: 0",
#     "maso_doanvien": "Unnamed: 1",
#     "hovaten": "Unnamed: 2",
#     "ngaysinh_nam": "Unnamed: 3",
#     "ngaysinh_nu": "Unnamed: 4",
#     "tinh": "Unnamed: 5",
#     "matinh": "Unnamed: 6",
#     "cmnd": "Unnamed: 7",
#     "huongluong_ngansach": "Unnamed: 8",
#     "huongluong_ngoaingansach": "Unnamed: 9",
#     "khongchuyentrach": "Unnamed: 10",
#     "chutich": "Unnamed: 11",
#     "phochutich": "Unnamed: 12",
#     "uvbch": "Unnamed: 13",
#     "totruong": "Unnamed: 14",
#     "topho": "Unnamed: 15",
#     "cb_cdcs": "Unnamed: 16",
#     "vuvbkt": "Unnamed: 17",
#     "ngayvao_congdoan": "Unnamed: 18",
#     "Unnamed_19": "Unnamed: 19",
#     "Unnamed_20": "Unnamed: 20",
#     "Unnamed_21": "Unnamed: 21",
#     "Unnamed_22": "Unnamed: 22",
#     "nguyenquan_tinh": "Unnamed: 23",
#     "nguyenquan_matinh": "Unnamed: 24",
# }
# mapping_cols_swap = {v: k for k, v in mapping_cols.items()}

class QueryEmployee(BaseModel):
    length: int


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return {"Data_App": "Success"}

@app.get("/imployee/get_all_employee_code")
async def get_all_employee_code():
    total_emp_code = main_data[["maso_doanvien", "hovaten"]].to_dict(orient="records")
    return total_emp_code


@app.post("/imployee/query")
async def employee_query(data: QueryEmployee):
    length = data.length
    if int(length) == -1:
        length = len(main_data)
    result = main_data.iloc[: int(length)]
    new_result = result.reset_index(drop=True)
    new_result_js = json.loads(new_result.to_json(orient="records"))
    pie_dict = gender_pie_chart(new_result)

    joining_by_gender_json = joining_date_by_gender(
        length, main_data
    )

    age_distribution = cal_age(length, main_data)
    provice_dis = province_distribution(length, main_data, top=15)
    
    output_dict = {
        "main_data": new_result_js,
        "pie_chart": pie_dict,
        "joining_by_gender": joining_by_gender_json,
        "age_distribution": age_distribution,
        "province_distribution": provice_dis,
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
    login_detail = loginHandler.login_request(user_name=email, password=password)
    if login_detail != None:
        return login_detail
    else:
        return None


class QueryEventList(BaseModel):
    userId: str

event_db_excel_path = "database/event/event_db.xlsx"
eventDBHandler = event_db_handler.EventDBHandler(event_db_excel_path)


@app.post("/WorkingApp/Event/EventData")
async def query_event_list(data: QueryEventList):
    userId = data.userId
    if userId == None:
        return None
    event_data_dict = eventDBHandler.query_evet_by_userId(userId=userId)
    return event_data_dict


class create_event_data(BaseModel):
    event_data: dict
    user_id: str


eventHandler = event_db_handler.EventHandler()


@app.post("/event/create")
async def create_event_handler(data: create_event_data):
    try:
        eventHandler.create_event(data.event_data, user_id=data.user_id)
        return {"message": "Create event success"}
    except Exception as e:
        return {"message": "Create event failed"}


@app.get("/event/query")
async def query_event_handler():
    try:
        event_data = eventHandler.query_all_events()
        return event_data
    except Exception as e:
        return {"message": "Query event failed"}


class update_event_data(BaseModel):
    event_data: dict
    user_id: str
    group_id: str



@app.post("/event/update")
async def upadte_event_handler(data: update_event_data):
    try:
        eventHandler.update_event_registing(data.user_id, data.group_id, data.event_data)
        return {"message": "Update event success"}
    except Exception as e:
        return {"message": "Update event failed"}


class query_event_data_model(BaseModel):
    event_id: str
    group_id: str


@app.post("/event/query_registed_data")
async def query_registed_event_data(data: query_event_data_model):
    # try:
        registed_data = eventHandler.query_registed_event_data(
            data.event_id, data.group_id
        )
        if registed_data != None:
            tables = registed_data["tables"]
            return tables
        else:
            return None
    # except Exception as e:
    #     return {"message": "Query event failed"}


class manager_query_table_model(BaseModel):
    table_id: str

@app.post("/event/query_registed_table_by_manager")
async def query_registed_event_data_manager(data: manager_query_table_model):
    # try:
        registed_table_df = eventHandler.get_table_data_by_manager(data.table_id)
        if registed_table_df.empty:
            return None
        temp_group_name = registed_table_df["group_id"].apply(
            lambda x: groupData.get_group_name(x)
        )
        registed_table_df["group_name"] = temp_group_name
        registed_table_df = registed_table_df.drop(columns=["event_id", "table_id", "key", "group_id"])
        # registed_table_df.rename(columns={"group_name": "group_id"}, inplace=True)
        registed_table = registed_table_df.to_dict(orient="records")
        return registed_table
    # except Exception as e:
    #     return {"message": "Create event failed"}


eventDashboardManager = event_db_handler.EventDashboardManager(employee_data_df=main_data, eventHandler=eventHandler, groupData=groupData)
@app.get("/event/query_total_stat_dashboard")
async def query_total_stat_dashboard():
    total_stat = eventDashboardManager.get_total_event_dashboard_stat()
    return total_stat




if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8005
        # ssl_version=ssl.PROTOCOL_TLS,
        # ssl_keyfile="/home/ubuntu/luongnam/working_space/working_app_python_backend/app/server.key",
        # ssl_certfile="/home/ubuntu/luongnam/working_space/working_app_python_backend/app/server.crt",
    )