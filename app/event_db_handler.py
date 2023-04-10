import pandas as pd
import ast
import os
import uuid
import datetime
import json
from  collections import defaultdict
import ast

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
    
    def update_event_registing(self, updating_user_id, update_data):
        data = self.try_reload_resigted_event_data()
        data[updating_user_id][update_data["id"]] = update_data
        with open(self.registed_event_data_path, 'w', encoding="utf-8") as f:
            print("FR)")
            json.dump(data, f, ensure_ascii=False, indent=4)

    def query_registed_event_data(self, event_id, user_id):
        data = self.try_reload_resigted_event_data()
        out_data = None
        try:
            out_data = data[user_id][event_id]
        except:
            print(f"Cannot query event {event_id} data from {user_id}")
        return out_data
    
    def query_registed_data_manager(self, event_id):
        data = self.try_reload_resigted_event_data()
        out_data = defaultdict(dict)
        # try:
        user_ids = data.keys()
        for each_user_id in user_ids:
            user_infor = loginHandler.get_user_info_by_id(each_user_id)
            if event_id in data[each_user_id].keys():
                tables_data = data[each_user_id][event_id]["tables"]
                new_tables_data = []
                for each_table in tables_data:
                    each_table_data = each_table["data"]
                    for idx in range(len(each_table_data)):
                        each_table_data[idx]["thêm bởi"] = user_infor["email"]
                    new_tables_data.append(each_table_data)
                out_data["tables"] = new_tables_data
        # except:
        #     print(f"Cannot query event {event_id}")
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