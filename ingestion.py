import io
import os
import time
from datetime import datetime, timedelta
import requests
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError
from dotenv import load_dotenv
import os

load_dotenv("creds.env")

# ==========================================
# 1. Configuration variables
# ==========================================
storage_acc = os.getenv("STORAGE_ACCOUNT_NAME")
storage_key = os.getenv("STORAGE_ACCOUNT_KEY")
container_name = os.getenv("CONTAINER_NAME")

# Production-grade headers to cleanly emulate a live human user session
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/"
}

# ==========================================
# 2. Azure Storage Connection Initialization
# ==========================================
def get_adls_container_client():
    """Establishes connection to the target ADLS Gen2 Storage Account Container."""
    account_url = f"https://{storage_acc}.dfs.core.windows.net"
    service_client = DataLakeServiceClient(account_url, credential=storage_key)
    return service_client.get_file_system_client(file_system=container_name)

# ==========================================
# 3. Dynamic "Time Traveler" URL Constructor
# ==========================================
def get_historical_bhavcopy_metadata(target_date):
    """
    Dynamically routes URL formulation and filename schema matching based 
    on the structural NSE infrastructure shift executed in July 2024.
    """
    year = target_date.strftime("%Y")
    month_txt = target_date.strftime("%b").upper() # e.g., 'MAR'
    month_num = target_date.strftime("%m")         # e.g., '03'
    
    # The dividing line when NSE migrated archive endpoints
    nse_migration_cutoff = datetime(2024, 7, 1)
    
    if target_date < nse_migration_cutoff:
        # ---- LEGACY ARCHIVE ROUTE (Pre-July 2024) ----
        day_str = target_date.strftime("%d")
        filename = f"cm{day_str}{month_txt}{year}bhav.csv.zip"
        url = f"https://archives.nseindia.com/content/historical/EQUITIES/{year}/{month_txt}/{filename}"
    else:
        # ---- MODERN ARCHIVE ROUTE (Post-July 2024 to Present) ----
        date_suffix = target_date.strftime("%Y%m%d")
        filename = f"BhavCopy_NSE_CM_0_0_0_{date_suffix}_F_0000.csv.zip"
        url = f"https://nsearchives.nseindia.com/content/cm/{filename}"
        
    # Standardized Hive-partitioned folder structure inside ADLS Gen2
    adls_path = f"landing/equities/year={year}/month={month_num}/{filename}"
    
    return url, filename, adls_path

# ==========================================
# 4. Core Ingestion Engine
# ==========================================
def download_and_land_bhavcopy(start_date, end_date):
    """
    Main loop execution layer. Validates landing targets, skips duplicates,
    manages stateless session barriers, and streams data payloads to Azure.
    """
    container_client = get_adls_container_client()
    
    # Initialize stateful HTTP session layer to persist security cookies
    session = requests.Session()
    session.headers.update(BROWSER_HEADERS)
    
    print("Seed Connection: Initializing session cookies via NSE Main Portal...")
    try:
        session.get("https://www.nseindia.com/", timeout=10)
        time.sleep(2)  # Human-emulation pause
        print("Initialization complete. Session validated successfully.\n")
    except Exception as e:
        print(f"CRITICAL: Initial cookie seeding handshake failed: {str(e)}")
        return

    current_date = start_date
    
    while current_date <= end_date:
        # Step A: Skip weekend records (Market Closed)
        if current_date.weekday() in (5, 6):
            current_date += timedelta(days=1)
            continue
            
        url, filename, adls_path = get_historical_bhavcopy_metadata(current_date)
        
        # Step B: Guardrail Check - Initialize file client reference
        file_client = container_client.get_file_client(adls_path)
        
        try:
            # Check if Azure storage already holds this exact historical record
            if file_client.exists():
                print(f" [Skipped] {filename} already exists securely in ADLS Gen2 landing.")
                current_date += timedelta(days=1)
                continue  # Skip web request entirely and jump to next loop
                
            print(f"Processing: {current_date.strftime('%Y-%m-%d')} -> Requesting URL...")
            response = session.get(url, timeout=15)
            
            # Step C: Evaluate Network Response Status Channels
            if response.status_code == 404:
                # Silently log market settlement/national trading holidays
                print(f" [Holiday] No file found for {filename} (Confirmed trading holiday).")
                
            elif response.status_code == 200:
                # Direct streaming transfer: memory buffer stream directly to Azure cloud
                file_client.upload_data(response.content, overwrite=True)
                print(f" [Success] Streamed {filename} directly into ADLS Gen2 container path.")
                
            elif response.status_code == 403:
                print(f" [Warning] Firewall 403 Forbidden flagged. Injecting extended backup sleep...")
                time.sleep(15)  # Back off to allow rate limiters to reset
                
            else:
                print(f" [Alert] Unexpected server return code encountered: {response.status_code}")
                
        except Exception as e:
            print(f" [Error Context] Pipeline fault recorded on execution date {current_date.strftime('%Y-%m-%d')}: {str(e)}")
            
        # Step D: Advance chronological pointer and apply defensive network throttling
        current_date += timedelta(days=1)
        time.sleep(1.5)  # 1.5-second pacing interval to prevent IP rating reduction

# ==========================================
# 5. Pipeline Entrypoint Execution
# ==========================================
if __name__ == "__main__":
    # Configure your structural backfill parameters here
    # To run a full 10-year backfill, adjust start to datetime(2016, 1, 1)
    start_backfill = datetime(2024, 6, 20)
    end_backfill = datetime(2024, 7, 10)
    
    print(f"Launching Resumable Phase 1 Historical Backfill Engine...")
    print(f"Targeting Processing Window: {start_backfill.strftime('%Y-%m-%d')} -> {end_backfill.strftime('%Y-%m-%d')}\n")
    
    download_and_land_bhavcopy(start_backfill, end_backfill)
    print("\nPhase 1 Resumable Ingestion Loop Completed Successfully.")