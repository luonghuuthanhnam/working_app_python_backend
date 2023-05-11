import sys
import os
import pandas as pd
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import FileResponse
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
    GroupData,
    working_status,
    sunburst_total_department,
)
from fastapi.middleware.cors import CORSMiddleware
import login_signup_handler as login_signup_handler
import event_db_handler as event_db_handler
import datetime
ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
ssl_context.load_cert_chain("server.crt", "server.key", "server.pem")
# ssl_context.load_cert_chain()
origins = [
    "http://localhost",
    "http://localhost:8000",
    "http://localhost:3000",
    "http://localhost:5000",
]

# employee_raw_data = pd.read_excel("database/DanhSachThongTinDoanVien.xlsx")
employee_data = pd.read_excel("database/employee_data/final_employee_data.xlsx", dtype=str)
groupData = GroupData("database/group_data.xlsx")
# main_data = employee_raw_data.iloc[5:]


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

@app.post("/imployee/query")
async def employee_query(data: QueryEmployee):
    length = data.length
    if int(length) <= 0:
        length = len(employee_data)
    result = employee_data.iloc[: int(length)]
    result["key"] = result["employee_id"].tolist()
    new_result = result.reset_index(drop=True)
    new_result = new_result.fillna("...")
    new_result_js = json.loads(new_result.to_json(orient="records"))
    pie_dict = gender_pie_chart(new_result)

    joining_by_gender_json = joining_date_by_gender(
        length, employee_data
    )

    age_distribution = cal_age(length, employee_data)
    provice_dis = province_distribution(length, employee_data, top=15)
    working_status_dict = working_status(length, employee_data)
    sunburst_data = sunburst_total_department(length, employee_data)
    output_dict = {
        "main_data": new_result_js,
        "pie_chart": pie_dict,
        "joining_by_gender": joining_by_gender_json,
        "age_distribution": age_distribution,
        "province_distribution": provice_dis,
        "working_status": working_status_dict,
        "sunburst_data": sunburst_data,
    }
    
    return output_dict

class update_employee_data(BaseModel):
    employee_data: list
    user_id: str

@app.post("/imployee/save_emp_data")
async def save_employee_data(data: update_employee_data):
    global employee_data
    backup_fname = datetime.datetime.now().strftime("%Y_%m_%d_%H")
    employee_data.to_excel(f"database/employee_data/backup_{backup_fname}.xlsx", index=False)

    emp_data = data.employee_data
    new_emp_df = pd.DataFrame.from_dict(emp_data)
    new_emp_df = new_emp_df.drop(columns=["key"])
    new_emp_df.to_excel("database/employee_data/final_employee_data.xlsx", index=False)
    employee_data = pd.read_excel("database/employee_data/final_employee_data.xlsx", dtype=str)


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


class GroupIDModel(BaseModel):
    groupID: str

@app.post("/imployee/get_all_employee_code")
async def get_all_employee_code(data: GroupIDModel):
    groupID = data.groupID
    print(groupID)
    group_data = employee_data[employee_data["group_id"] == groupID]
    total_emp_code = group_data[["employee_id", "hovaten", "ngaysinh"]].to_dict(orient="records")
    return total_emp_code

class UserIDModel(BaseModel):
    userId: str

event_db_excel_path = "database/event/event_db.xlsx"
eventDBHandler = event_db_handler.EventDBHandler(event_db_excel_path)


@app.post("/WorkingApp/Event/EventData")
async def query_event_list(data: UserIDModel):
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


class UserIdGroupIdModel(BaseModel):
    event_id: str
    group_id: str


@app.post("/event/query_registed_data")
async def query_registed_event_data(data: UserIdGroupIdModel):
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
        registed_table_df = registed_table_df[(registed_table_df["employee_id"] != None) & (registed_table_df["employee_id"] != "...")]
        # registed_table_df.rename(columns={"group_name": "group_id"}, inplace=True)
        registed_table = registed_table_df.to_dict(orient="records")
        return registed_table
    # except Exception as e:
    #     return {"message": "Create event failed"}

class DashBoardQuery(BaseModel):
    user_id: str
    group_id: str
    time_range: list

eventDashboardManager = event_db_handler.EventDashboardManager(employee_data_df=employee_data, eventHandler=eventHandler, groupData=groupData)
@app.post("/event/query_total_stat_dashboard")
async def query_total_stat_dashboard(data: DashBoardQuery):
    time_range = data.time_range
    total_stat = eventDashboardManager.get_total_event_dashboard_stat(time_range=time_range)
    return total_stat

@app.post("/event/query_department_stat_dashboard")
async def query_department_stat_dashboard(data: DashBoardQuery):
    department_id = data.group_id
    time_range = data.time_range
    # total_stat = eventDashboardManager.get_total_event_dashboard_stat()
    total_stat = eventDashboardManager.get_department_event_dashboard_stat(group_id=department_id, time_range=time_range)
    return total_stat


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        print(file.filename)
        contents = await file.read()
        file_path = os.path.join("data", file.filename)
        with open(file_path, "wb") as f:
            f.write(contents)
        return {"message": "Upload success", "filename": file.filename, "file_path": file_path}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Upload failed")
    
@app.get("/download/{file_path:path}")
async def download_file(file_path: str):
    print(file_path)
    new_path = "data/" + file_path
    return FileResponse(new_path)

@app.post("/event/get_department_list")
async def upadte_event_handler(data: UserIDModel):
    groupData.reload()
    group_data = groupData.get_all_group_id_name()
    return group_data
    # try:
    #     eventHandler.update_event_registing(data.user_id, data.group_id, data.event_data)
    #     return {"message": "Update event success"}
    # except Exception as e:
    #     return {"message": "Update event failed"}

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        timeout_keep_alive=60,
        ssl_version=ssl.PROTOCOL_TLS,
        ssl_keyfile="server.key",
        ssl_certfile="server.crt",
    )

