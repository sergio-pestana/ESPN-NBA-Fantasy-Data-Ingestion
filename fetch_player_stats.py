from league_vars import league

# Data Manipulation
import numpy as np
import pandas as pd


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


if __name__ == "__main__":
    print(get_player_stats())