import os
import pandas as pd
from datetime import datetime
import re


def excel_to_csv(excel_file_path: str, output_dirs: dir) -> None:
    BR_pattern= r"^(?:Billing Report )?(\d{1,2}) (\w{3}) (\d{4})$"
    
    try:
        excel_sheets_df= pd.read_excel(excel_file_path, sheet_name=None)
        
        for sheet_name, sheet_df in excel_sheets_df.items():
            
            ba_match= re.match(BR_pattern, sheet_name)
            ba_flag= bool(ba_match)
            
            if ba_match:
                d,m,y = ba_match.groups()
                date_obj = datetime.strptime(f"{d} {m} {y}", "%d %b %Y")
                formatted_date = date_obj.strftime("%Y%m%d")
                formatted_name= f"Billing-Report-{formatted_date}"
            else:
                formatted_name= "Project-Allocation"
                
            valid_sheet_name =f"{datetime.today().strftime('%Y%m%d') +"-" +formatted_name}.csv"
            
            csv_path= os.path.join(output_dirs['billing-reports' if ba_flag else 'project-allocations'], valid_sheet_name)
            
            sheet_df.to_csv(csv_path, index=False)
            
            print(f"Converted {sheet_name} to CSV: {csv_path}")
            
    except Exception as e:
        print(f"Error converting{excel_file_path}: {e}")
    return