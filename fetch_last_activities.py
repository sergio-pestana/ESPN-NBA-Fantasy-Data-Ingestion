from league_vars import league

# Data manipulation
from datetime import datetime
import pandas as pd
import hashlib

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
    
if __name__ == "__main__":
    print(get_last_activities())