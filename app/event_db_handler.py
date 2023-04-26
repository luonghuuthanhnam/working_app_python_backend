import pandas as pd
import ast
import os
import uuid
import datetime
import json
from  collections import defaultdict
import ast
import numpy as np

class EventDBHandler():
    def __init__(self, excel_data_path) -> None:
        self.event_db = pd.read_excel(excel_data_path, dtype=str)
        self.event_db = self.event_db.fillna("")

    def query_evet_by_userId(self, userId):
        event_df = self.event_db.copy()
        requested_events = event_df[event_df["requesting_member_id"].str.contains(userId, na=False)]
        accepted_events = requested_events[requested_events["accepted_member_id"].str.contains(userId, na=False)]
        rejected_events = requested_events[requested_events["rejected_member_id"].str.contains(userId, na=False)]
        pendding_indexes = [each for each in list(requested_events.index)
                            if each not in (list(accepted_events.index) +
                                            list(rejected_events.index))]
        pending_events = requested_events.loc[pendding_indexes]

        result = {
            "pending_events": pending_events.to_dict("list"),
            "accepted_events": accepted_events.to_dict("list"),
            "rejected_events": rejected_events.to_dict("list"),
        }
        return result

    def seperate_id_in_event(self, string_like_list):
        out_list = ast.literal_eval(string_like_list)
        return out_list

    def accept_joining_event(self, user_id, event_id):
        return None

    def reject_joining_event(self, user_id, event_id):
        return None

from login_signup_handler import LoginHandler
loginHandler = LoginHandler()
class EventHandler():
    def __init__(self, event_db_excel_file = "database/event/event_db.xlsx", registed_event_data_path = "database/event/registed_event_data.json") -> None:
        self.registed_event_data_path = registed_event_data_path
        self.event_db_excel_file = event_db_excel_file
        if not os.path.exists(self.event_db_excel_file):
            self.create_event_db_excel_file(event_db_excel_file)
        self.event_db = pd.read_excel(self.event_db_excel_file, dtype=str)
        self.event_db = self.event_db.fillna("")

    def create_event_db_excel_file(self, event_db_excel_file = "database/event/event_db.xlsx"):
        event_db = pd.DataFrame(columns=["event_id", "created_by", "created_at", "event_data"])
        event_db.to_excel(event_db_excel_file, index=False)

    def create_event(self, event_data, user_id):
        event_id = str(uuid.uuid4())
        created_by = user_id
        created_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        saving_event_data = {
            "event_id": event_id,
            "event_title": event_data["title"],
            "created_by": created_by,
            "created_at": created_at,
            "event_data": event_data
        }
        event_db = self.event_db.copy()
        event_db = event_db.append(saving_event_data, ignore_index=True)
        event_db.to_excel(self.event_db_excel_file, index=False)
        self.event_db = event_db
        event_db.to_excel(self.event_db_excel_file, index=False)
        return event_db
    
    def reload_event_db(self):
        self.event_db = pd.read_excel(self.event_db_excel_file, dtype=str)
        self.event_db = self.event_db.fillna("")

    def query_all_events(self):
        self.reload_event_db()
        event_db = self.event_db.copy()
        return event_db.to_dict()
    
    def try_reload_resigted_event_data(self):
        try:
            with open(self.registed_event_data_path, 'r', encoding="utf-8") as f:
                data = json.load(f)
                data = defaultdict(dict,data)
        except FileNotFoundError:
            data = defaultdict(dict)
        except json.decoder.JSONDecodeError:
            data = defaultdict(dict)
        return data
    
    def update_event_registing(self, updating_user_id, group_id, update_data):
        with open(f"database/event/each_group/{group_id}.json", 'w', encoding="utf-8") as f:
            dump_data = {group_id: update_data}
            json.dump(dump_data, f, ensure_ascii=False, indent=4)
        data = self.try_reload_resigted_event_data()
        data[group_id][update_data["id"]] = update_data
        with open(self.registed_event_data_path, 'w', encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    def query_registed_event_data(self, event_id, group_id):
        data = self.try_reload_resigted_event_data()
        out_data = None
        try:
            out_data = data[group_id][event_id]
        except:
            print(f"Cannot query event {event_id} data from {group_id}")
        return out_data
    
    def get_table_names_in_event(self, event_id):
        event_db = self.event_db.copy()
        event_data = event_db[event_db["event_id"] == event_id]
        selected_event_data = event_data["event_data"].values[0]
        if type(selected_event_data) == str:
            selected_event_data = ast.literal_eval(selected_event_data)
        
        table_names = [each["name"] for each in selected_event_data["tables_data"]]
        # table_names = event_data["table_names"]
        return table_names
    
    def transform_table_data(self):
        raw_data = json.load(open(self.registed_event_data_path, "r", encoding="utf-8"))
        user_ids = raw_data.keys()

        total_table_data = defaultdict(list)
        for each_user in user_ids:
            group_data = raw_data[each_user]
            for each_event in group_data:
                event_data = group_data[each_event]
                for each_table in event_data["tables"]:
                    table_id = each_table["table_id"]
                    table_data = each_table["data"]
                    for each_row in table_data:
                        cur_row = each_row
                        cur_row["event_id"] = each_event
                        cur_row["group_id"] = each_user
                        cur_row["table_id"] = table_id
                        total_table_data[table_id].append(cur_row)
        return total_table_data
    def get_table_data_by_manager(self, selected_table_id):
        total_table_data = self.transform_table_data()
        table_df = pd.DataFrame.from_dict(total_table_data[selected_table_id])
        no_group_id_table_df = table_df.drop(columns=["group_id", "event_id", "table_id", "key"])
        no_group_id_table_df = no_group_id_table_df.replace("...", np.nan)
        no_group_id_table_df.dropna(how="all",inplace=True)
        cleaned_table_df = table_df.iloc[no_group_id_table_df.index.to_list()]
        return cleaned_table_df
    
    def event_id_to_name(self, event_id):
        event_db = self.event_db.copy()
        event_data = event_db[event_db["event_id"] == event_id]
        event_name = event_data["event_title"].values[0]
        return event_name

class EventDashboardManager():
    def __init__(self, employee_data_df, eventHandler, groupData) -> None:
        self.employee_data_df = employee_data_df
        self.eventHandler = eventHandler
        self.groupData = groupData

    def get_involved_emp_df(self, emp_code_list):
        dfs = [self.employee_data_df[self.employee_data_df['maso_doanvien'] == code] for code in emp_code_list]
        return pd.concat(dfs)

    def get_gender_ratio(self, input_df):
        gender = input_df["gioitinh"].value_counts()
        count_male = gender["nam"]
        count_female = gender["nu"]
        return count_male, count_female

    def get_avg_age(self, input_df):
        dob = input_df["ngaysinh"].to_list()
        cur_year = int(datetime.datetime.now().year)
        ages = [cur_year - int(dob[i].split("/")[-1]) for i in range(len(dob))]
        avg_age = sum(ages) / len(ages)
        return avg_age

    def get_total_event_dashboard_stat(self):

        query_event_data = self.eventHandler.query_all_events()
        event_ids = query_event_data["event_id"]
        event_datas = query_event_data["event_data"]
        total_event = len(event_ids)
        total_table = 0
        for idx, str_data in event_datas.items():
            data = str_data
            if type(str_data) == str:
                data = ast.literal_eval(str_data)
            tables = data["tables_data"]
            total_table += len(tables)

        total_data = self.eventHandler.transform_table_data()
        total_rows = []
        for each_key, each_data in total_data.items():
                total_rows+=each_data
        total_data_df = pd.DataFrame.from_dict(total_rows)
        total_joining_emp_code = total_data_df["Mã nhân viên"].tolist()
        total_joining_emp_code = [value for value in total_joining_emp_code if value != "..."]
        joining_emp_df = self.get_involved_emp_df(total_joining_emp_code)

        total_joining_employee = len(set(total_data_df["Mã nhân viên"]))
        # total_event = len(set(total_data_df["event_id"]))
        # total_table = len(set(total_data_df["table_id"]))
        male, female = self.get_gender_ratio(joining_emp_df)
        avg_age = self.get_avg_age(joining_emp_df)

        #Counting joining number of employee in each group
        joining_emp_by_group = []
        short_df = total_data_df[["group_id", "Mã nhân viên"]]
        short_df = short_df[short_df["Mã nhân viên"] != "..."]
        group_id_count = short_df["group_id"].value_counts().to_dict()
        group_names = ["N/A"]*3
        most_joining_values = ["0"]*3
        most_joining_groups = sorted(group_id_count, key=group_id_count.get, reverse=True)
        for i in range(len(most_joining_groups)):
            if i == 3:
                break
            group_names[i] = self.groupData.get_group_name(most_joining_groups[i])
        
        for i in range(len(most_joining_groups)):
            if i == 3:
                break
            most_joining_values[i] = str(group_id_count[most_joining_groups[i]])
        mvp_emp_joining_group = {
            "group_names": group_names,
            "values": most_joining_values,
        }
        for each_group_id, each_value in group_id_count.items():
            temp_dict = {}
            group_name = self.groupData.get_group_name(each_group_id)
            temp_dict["type"] = group_name
            temp_dict["value"] = each_value
            joining_emp_by_group.append(temp_dict)
            # print(f"{self.groupData.get_group_name(each_group_id)}: {each_value}")

        #Counting joining event of each group
        event_joining_by_group = []
        short_df = total_data_df[["group_id", "Mã nhân viên", "event_id"]]
        short_df = short_df[short_df["Mã nhân viên"] != "..."]
        short_df = short_df[["group_id", "event_id"]]
        x = short_df.groupby(["group_id", "event_id"]).count()
        group_ids = short_df["group_id"].unique().tolist()
        for each_group_id in group_ids:
            temp_dict = {}
            group_name = self.groupData.get_group_name(each_group_id)
            temp_dict["type"] = group_name
            temp_dict["value"] = len(x.loc[each_group_id])
            event_joining_by_group.append(temp_dict)
            # print(f"{self.groupData.get_group_name(each_group_id)}: {len(x.loc[each_group_id])}")

        short_df = total_data_df[["Mã nhân viên", "table_id"]]
        short_df = short_df[short_df["Mã nhân viên"] != "..."]
        count_df = short_df.groupby('Mã nhân viên').count()
        top_3_emp = count_df.sort_values(by='table_id', ascending=False).head(3)
        top_3_emp = top_3_emp.reset_index()
        top_3_emp_ids = top_3_emp["Mã nhân viên"].tolist()
        top_3_emp_df = self.get_involved_emp_df(top_3_emp_ids)
        top_3_emp_names = top_3_emp_df["hovaten"].tolist()
        top_3_emp_values = top_3_emp["table_id"].tolist()
        top_3_emp_data = {
            "emp_names": top_3_emp_names,
            "values": top_3_emp_values
        }
        output_dict = {
            "total_joining_employee": int(total_joining_employee),
            "total_event": int(total_event),
            "total_table": int(total_table),
            "male": int(male),
            "female": int(female),
            "avg_age": round(float(avg_age),1),
            "emp_joining_by_group": joining_emp_by_group,
            "event_joining_by_group": event_joining_by_group,
            "mvp_emp_joining_group": mvp_emp_joining_group,
            "top_3_emp_data": top_3_emp_data
            }
        return output_dict