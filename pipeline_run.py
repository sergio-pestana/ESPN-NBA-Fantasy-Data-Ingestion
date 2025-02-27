#!/usr/bin/env python
"""
Simple Fantasy Basketball Data Collection Pipeline

This script runs your ESPN data collection modules in sequence,
providing basic logging and error handling.
"""

import os
import sys
import time
import logging
from datetime import datetime
import subprocess
from dotenv import load_dotenv

# Set up basic logging - console only for simplicity
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("fantasy_pipeline")

# Load environment variables
load_dotenv()

# Config
SCRIPTS_DIR = 'src'  # Adjust this to match your project structure
DATA_SCRIPTS = [
    'league_vars.py',
    'fetch_last_activities.py',
    'fetch_boxscores.py',
    'fetch_player_stats.py',
    'fetch_teams.py'
]

def check_environment():
    """Verify that required ESPN API credentials exist"""
    required_vars = ['ESPN_S2', 'SWID', 'LEAGUE_ID'] 
    missing = [var for var in required_vars if not os.environ.get(var)]
    
    if missing:
        logger.error(f"Missing environment variables: {', '.join(missing)}")
        logger.error("Please set these in your .env file")
        return False
    return True

def run_data_collection():
    """Run each data collection script in sequence"""
    success_count = 0
    
    for script_name in DATA_SCRIPTS:
        script_path = os.path.join(SCRIPTS_DIR, script_name)
        
        # Skip if script doesn't exist
        if not os.path.exists(script_path):
            logger.warning(f"Script not found: {script_path}")
            continue
            
        logger.info(f"Running {script_name}...")
        start_time = time.time()
        
        try:
            # Run the script and capture output
            result = subprocess.run(
                [sys.executable, script_path],
                check=True,
                capture_output=True,
                text=True
            )
            duration = time.time() - start_time
            logger.info(f"✓ {script_name} completed successfully in {duration:.2f} seconds")
            success_count += 1
            
            # Optionally show script output for debugging
            if result.stdout:
                logger.debug(f"Output from {script_name}:\n{result.stdout}")
                
        except subprocess.CalledProcessError as e:
            logger.error(f"✗ {script_name} failed with exit code {e.returncode}")
            if e.stdout:
                logger.info(f"Standard output:\n{e.stdout}")
            if e.stderr:
                logger.error(f"Error output:\n{e.stderr}")
                
    return success_count, len(DATA_SCRIPTS)

def main():
    """Main pipeline function"""
    logger.info(f"Starting fantasy basketball data collection at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Environment check
    if not check_environment():
        sys.exit(1)
    
    # Run data collection
    successes, total = run_data_collection()
    
    # Report results
    if successes == total:
        logger.info(f"✅ All done! {successes}/{total} scripts completed successfully")
    elif successes > 0:
        logger.warning(f"⚠️ Partial success: {successes}/{total} scripts completed")
    else:
        logger.error(f"❌ Pipeline failed: 0/{total} scripts completed")


def run_dbt_transformations(full_refresh=False):
    """Run dbt models to transform the data"""
    original_dir = os.getcwd()
    
    try:
        # Change to nbafantasy directory
        dbt_dir = 'nbafantasy'
        profiles_path = os.path.join(original_dir, 'profiles.yml')
        
        if not os.path.exists(profiles_path):
            logger.error(f"profiles.yml not found at {profiles_path}")
            return False
            
        os.chdir(dbt_dir)
        logger.info(f"Changed to dbt project directory: {os.getcwd()}")
        
        # Install dependencies with explicit profiles path
        logger.info("Installing dbt dependencies...")
        subprocess.run(['dbt', 'deps', '--profiles-dir', original_dir], 
                      check=True, capture_output=True, text=True)
        
        # Run dbt models
        cmd = ['dbt', 'run', '--profiles-dir', original_dir]
        if full_refresh:
            cmd.append('--full-refresh')
            
        logger.info(f"Running dbt models: {' '.join(cmd)}")
        start_time = time.time()
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        duration = time.time() - start_time
        
        logger.info(f"✓ dbt models completed successfully in {duration:.2f} seconds")
        
        # Run dbt tests with explicit profiles path
        logger.info("Running dbt tests...")
        test_result = subprocess.run(
            ['dbt', 'test', '--profiles-dir', original_dir], 
            check=True, capture_output=True, text=True
        )
        
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"✗ dbt operation failed with exit code {e.returncode}")
        if e.stdout: logger.info(f"Output: {e.stdout}")
        if e.stderr: logger.error(f"Error: {e.stderr}")
        return False
    finally:
        os.chdir(original_dir)


if __name__ == "__main__":
    main()
    run_dbt_transformations()