from league_vars import league

# Data Manipulation
import numpy as np
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


def get_player_stats():
    all_players = []

    # Get roster players
    for team in league.teams:
        for player in team.roster:
            try:
                player_info = {
                        'id': player.playerId,
                        'name': player.name,
                        'position': player.position,
                        'posRank': player.posRank,
                        'proTeam': player.proTeam,
                        'status': 'In Roster',
                        'team': team.team_name,
                        'avg_stats': player.nine_cat_averages,
                        '2025_stats': player.stats['2025_total']['avg'],
                        '7d_stats': player.stats['2025_last_7']['avg'],
                        '15d_stats': player.stats['2025_last_15']['avg'],
                        '30d_stats': player.stats['2025_last_30']['avg'],
                        'injured': player.injured
                }
                all_players.append(player_info)

            except:
                player_info = {
                        'id': player.playerId,
                        'name': player.name,
                        'position': player.position,
                        'posRank': player.posRank,
                        'proTeam': player.proTeam,
                        'status': 'In Roster',
                        'team': team.team_name,
                        'avg_stats': player.nine_cat_averages,
                        '2025_stats': np.nan,
                        '7d_stats': np.nan,
                        '15d_stats': np.nan,
                        '30d_stats': np.nan,
                        'injured': player.injured
                }
                all_players.append(player_info)

    # Get free agents
    free_agents = league.free_agents(size=100) 
    for player in free_agents:
        try:
            player_info = {
                    'id': player.playerId,
                    'name': player.name,
                    'position': player.position,
                    'posRank': player.posRank,
                    'proTeam': player.proTeam,
                    'status': 'In Roster',
                    'team': team.team_name,
                    'avg_stats': player.nine_cat_averages,
                    '2025_stats': player.stats['2025_total']['avg'],
                    '7d_stats': player.stats['2025_last_7']['avg'],
                    '15d_stats': player.stats['2025_last_15']['avg'],
                    '30d_stats': player.stats['2025_last_30']['avg'],
                    'injured': player.injured
            }
            all_players.append(player_info)

        except:
            player_info = {
                    'id': player.playerId,
                    'name': player.name,
                    'position': player.position,
                    'posRank': player.posRank,
                    'proTeam': player.proTeam,
                    'status': 'In Roster',
                    'team': team.team_name,
                    'avg_stats': player.nine_cat_averages,
                    '2025_stats': {},
                    '7d_stats': {},
                    '15d_stats': {},
                    '30d_stats': {},
                    'injured': player.injured
            }
            all_players.append(player_info)


    return pd.DataFrame(all_players)

def convert_custom_types(df):
    df = df.copy()
    for col in df.columns:
        if df[col].apply(lambda x: hasattr(x, '__str__')).any():
            df[col] = df[col].astype(str)
    return df
    
def save_to_postgres(df, schema='public'):
    df = convert_custom_types(df)
    df.to_sql('player_stats', engine, if_exists='replace', schema=schema)


if __name__ == "__main__":
    player_stats_df = get_player_stats()
    save_to_postgres(player_stats_df, schema='public')
    print('Done')
    