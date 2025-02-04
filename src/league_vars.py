# Import parameters
import os
from dotenv import load_dotenv

load_dotenv()
espn_s2 = os.environ.get('ESPN_S2')
swid = os.environ.get('SWID')
league_id = os.environ.get('LEAGUE_ID')

# Basketball API
from espn_api.basketball import League
league = League(league_id=league_id, year=2025, espn_s2=espn_s2, swid=swid)