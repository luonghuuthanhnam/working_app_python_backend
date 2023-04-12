import pandas as pd
class LoginHandler():
    def __init__(self) -> None:
        self.excel_db = pd.read_excel("database/login_auth.xlsx")
    
    def login_request(self, user_name, password):
        result = self.excel_db[(self.excel_db["email"]==user_name) & (self.excel_db["password"]==password)]
        user_id = None
        group_id = None
        user_name = None
        if len(result) > 0:
            user_id = result.iloc[0]["user_id"]
            group_id = result.iloc[0]["group_id"]
            user_name = result.iloc[0]["user_name"]
            output = {
                "user_id": user_id,
                "group_id": group_id,
                "user_name": user_name
            }
        else:
            output = None
        return output

    def get_user_info_by_id(self, user_id):
        result = self.excel_db[self.excel_db["user_id"]==user_id]
        if len(result) > 0:
            user_info = result.iloc[0]
        else:
            user_info = None
        return user_info