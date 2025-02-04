from league_vars import league

# Data manipulation
from datetime import datetime
import pandas as pd
import hashlib
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os


DB_HOST=os.getenv('DB_HOST_PROD')
DB_PORT=os.getenv('DB_PORT_PROD')
DB_NAME=os.getenv('DB_NAME_PROD')
DB_USER=os.getenv('DB_USER_PROD')
DB_PASS=os.getenv('DB_PASS_PROD')
DB_SCHEMA=os.getenv('DB_SCHEMA_PROD')

DATABASE_URL = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}'
engine = create_engine(DATABASE_URL)

def get_last_activities():
    activities = league.recent_activity()
    try:
        activities_data = []
        for activity in activities:
            if len(activity.actions)!=0:

                combined_inputs = f"{activity.date}||{activity.actions}"
                custom_id = hashlib.sha256(combined_inputs.encode('utf-8')).hexdigest()
                df = pd.DataFrame(activity.actions, columns=['team', 'type', 'player_name', 'notes'])
                df['id'] = custom_id
                df['processed_at'] = datetime.fromtimestamp(activity.date/1000).strftime('%Y-%m-%d %H:%M:%S')
                df['team'] = df['team'].astype(str).str.replace('Team(', '').str.replace(')', '')
                print()
                activities_data.append(df)
            else:
                next
        full_df = pd.concat(activities_data, axis=0, ignore_index=True)
        
        return full_df[['id', 'processed_at', 'team', 'type', 'player_name', 'notes']]

    except Exception as e:
        print(f"Error creating DataFrame: {str(e)}")
        return None

def convert_custom_types(df):
    df = df.copy()
    for col in df.columns:
        if df[col].apply(lambda x: hasattr(x, '__str__')).any():
            df[col] = df[col].astype(str)
    return df
    
def save_to_postgres(df, schema='public'):
    df = convert_custom_types(df)
    df.to_sql('last_activities', engine, if_exists='append', schema=schema)
    
if __name__ == "__main__":
    last_activities_df = get_last_activities()
    save_to_postgres(last_activities_df, schema='public')
    print('Done')