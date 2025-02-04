from league_vars import league

# Data Manipulation
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST=os.getenv('DB_HOST_PROD')
DB_PORT=os.getenv('DB_PORT_PROD')
DB_NAME=os.getenv('DB_NAME_PROD')
DB_USER=os.getenv('DB_USER_PROD')
DB_PASS=os.getenv('DB_PASS_PROD')
DB_SCHEMA=os.getenv('DB_SCHEMA_PROD')

DATABASE_URL = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}'
engine = create_engine(DATABASE_URL)


def get_teams():
# Get a team
    try:
        # Create a list to store team data
        teams_data = []
        
        # Collect data for each team
        for team in league.teams:
            team_dict = {
                'Team Name': team.team_name,
                'Owner': f"{team.owners[0]['firstName']} {team.owners[0]['lastName']}",
                'Team ID': team.team_id,
                'Abbreviation': team.team_abbrev,
                'Division ID': team.division_id,
                'Division Name': team.division_name,
                'Total Wins': team.wins,
                'Total Losses': team.losses,
                'Standing': team.standing
            }
            teams_data.append(team_dict)
            
        print(f"Total of {len(league.teams)} added.")
        return pd.DataFrame(teams_data)
    
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
    df.to_sql('teams', engine, if_exists='replace', schema=schema)

if __name__ == "__main__":
    teams_df = get_teams()
    save_to_postgres(teams_df, schema='public')
    print('Done')