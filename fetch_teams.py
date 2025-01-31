from league_vars import league

# Data Manipulation
import pandas as pd


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

if __name__ == "__main__":
    print(get_teams())