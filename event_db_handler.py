import pandas as pd
import ast
import os
import uuid
import datetime

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


class EventHandler():
    def __init__(self, event_db_excel_file = "database/event/event_db.xlsx") -> None:
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
        event_data = {
            "event_id": event_id,
            "created_by": created_by,
            "created_at": created_at,
            "event_data": event_data
        }
        event_db = self.event_db.copy()
        event_db = event_db.append(event_data, ignore_index=True)
        event_db.to_excel(self.event_db_excel_file, index=False)
        self.event_db = event_db
        event_db.to_excel(self.event_db_excel_file, index=False)
        return event_db
    
    