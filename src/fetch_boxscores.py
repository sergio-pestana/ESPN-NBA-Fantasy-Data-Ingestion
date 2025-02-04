from league_vars import league

# Data Manipulation
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

def get_matches_boxscores():
    try:
        matches_boxscore = []

        for week in range(1,league.currentMatchupPeriod+1):
            boxscores = league.box_scores(week)
            for boxscore in boxscores:
                combined = f"{week}||{boxscore}"
                boxscore_dict = {
                    'id': hashlib.sha256(combined.encode('utf-8')).hexdigest(),
                    'week': week,
                    'match_header': boxscore,
                    ## ---- HOME TEAM -----
                    'home_team': boxscore.home_team,
                    'home_wins': boxscore.home_wins,
                    'home_losses': boxscore.home_losses,
                    'home_ties': boxscore.home_ties,
                    'home_pts': boxscore.home_stats['PTS']['value'],
                    'home_pts_output': boxscore.home_stats['PTS']['result'],
                    'home_blk': boxscore.home_stats['BLK']['value'],
                    'home_blk_output': boxscore.home_stats['BLK']['result'],
                    'home_3pm': boxscore.home_stats['3PM']['value'],
                    'home_3pm_output': boxscore.home_stats['3PM']['result'],
                    'home_stl': boxscore.home_stats['STL']['value'],
                    'home_stl_output': boxscore.home_stats['STL']['result'],
                    'home_ast': boxscore.home_stats['AST']['value'],
                    'home_ast_output': boxscore.home_stats['AST']['result'],
                    'home_reb': boxscore.home_stats['REB']['value'],
                    'home_reb_output': boxscore.home_stats['REB']['result'],
                    'home_to': boxscore.home_stats['TO']['value'],
                    'home_to_output': boxscore.home_stats['TO']['result'],
                    ## ---- AWAY TEAM ----
                    'away_team':boxscore.away_team,
                    'away_wins':boxscore.away_wins,
                    'away_losses':boxscore.away_losses,
                    'away_ties':boxscore.away_ties,
                    'away_pts': boxscore.away_stats['PTS']['value'],
                    'away_pts_output': boxscore.away_stats['PTS']['result'],
                    'away_blk': boxscore.away_stats['BLK']['value'],
                    'away_blk_output': boxscore.away_stats['BLK']['result'],
                    'away_3pm': boxscore.away_stats['3PM']['value'],
                    'away_3pm_output': boxscore.away_stats['3PM']['result'],
                    'away_stl': boxscore.away_stats['STL']['value'],
                    'away_stl_output': boxscore.away_stats['STL']['result'],
                    'away_ast': boxscore.away_stats['AST']['value'],
                    'away_ast_output': boxscore.away_stats['AST']['result'],
                    'away_reb': boxscore.away_stats['REB']['value'],
                    'away_reb_output': boxscore.away_stats['REB']['result'],
                    'away_to': boxscore.away_stats['TO']['value'],
                    'away_to_output': boxscore.away_stats['TO']['result'],
                    ## ---- OTHER ----
                    'match_output': boxscore.winner
                }
                matches_boxscore.append(boxscore_dict)
        return pd.DataFrame(matches_boxscore)

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
    df.to_sql('boxscores', engine, if_exists='replace', schema=schema)
    
if __name__ == "__main__":
    boxscores_df = get_matches_boxscores()
    save_to_postgres(boxscores_df, schema='public')
    print('Done')
