import pandas as pd
class LoginHandler():
    def __init__(self) -> None:
        self.excel_db = pd.read_excel("app/database/login_auth.xlsx")
    
    def login_request(self, user_name, password):
        result = self.excel_db[(self.excel_db["email"]==user_name) & (self.excel_db["password"]==password)]
        if len(result) > 0:
            user_id = result.iloc[0]["user_id"]
        else:
            user_id = None
        return user_id