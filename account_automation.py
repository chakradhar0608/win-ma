import asyncio
import csv
import json
import os
import random
from datetime import datetime
from playwright.async_api import async_playwright

# ================= CONFIG =================
MAX_WORKERS = 1  # Keep 1 per shard to avoid IP bans
MAX_RETRIES = 2

ACCOUNTS_FILE = "accounts.csv"
RESULTS_FILE = "account_balances.csv"
FAILED_FILE = "failed_accounts.csv"     # <--- NEW: Separate file for failures
SELECTORS_FILE = "selectors.json"
SCREENSHOTS_DIR = "screenshots"

# ================= GLOBALS =================
csv_lock = asyncio.Lock()
failed_lock = asyncio.Lock()
stats_lock = asyncio.Lock()

STATS = {
    "total": 0,
    "processed": 0,
    "success": 0,
    "failed": 0
}

# ================= HELPERS =================
async def log_progress():
    async with stats_lock:
        print(f"[PROGRESS] {STATS['processed']}/{STATS['total']} | "
              f"Success: {STATS['success']} | Failed: {STATS['failed']}", flush=True)

async def save_result(row):
    """Saves all attempts (Success and Failure) to the main log."""
    async with csv_lock:
        file_exists = os.path.exists(RESULTS_FILE)
        with open(RESULTS_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["username", "password", "balance", "status", "error", "timestamp"])
            if not file_exists: writer.writeheader()
            row["timestamp"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            writer.writerow(row)

async def save_failed(row):
    """Saves ONLY failed accounts to a separate CSV for easy retrying."""
    async with failed_lock:
        file_exists = os.path.exists(FAILED_FILE)
        with open(FAILED_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["username", "password", "error", "timestamp"])
            if not file_exists: writer.writeheader()
            
            # Simplified row for the failed file
            failed_row = {
                "username": row["username"],
                "password": row["password"],
                "error": row["error"],
                "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            }
            writer.writerow(failed_row)

# ================= STEALTH & POPUPS =================
async def apply_stealth(page):
    """Manually hides bot signals."""
    await page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        if (!window.chrome) { window.chrome = { runtime: {} }; }
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
            Promise.resolve({ state: 'denied' }) :
            originalQuery(parameters)
        );
        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-US', 'en'],
        });
    """)

async def dismiss_overlays(page, username=""):
    """Aggressively closes popups."""
    # 1. JS Removal
    try:
        await page.evaluate("""() => {
            const ids = ["strEchApp_ovrlay", "aviatrix-container_overlay", "mainPopupWrpr", "popup-overlay", "modal-overlay", "app-download-popup", "switchuser_riv"];
            ids.forEach(id => { const el = document.getElementById(id); if (el) el.remove(); });
            const classes = ["modal-backdrop", "fade", "show", "overlay", "popup-container"];
            classes.forEach(cls => { const els = document.getElementsByClassName(cls); for(let i=0; i<els.length; i++) els[i].remove(); });
        }""")
    except: pass

    # 2. Button Click
    close_patterns = [
        "button.animCLseBtn", "button.mnPopupClose", ".popup-close", ".modal-close",
        "button[aria-label='Close']", "[class*='close']", ".close-btn", ".pgSoftClsBtn"
    ]
    for selector in close_patterns:
        try:
            if await page.locator(selector).first.is_visible():
                await page.locator(selector).first.click(timeout=500)
                await asyncio.sleep(0.2)
        except: pass

# ================= CORE LOGIC =================
async def process_account(browser, account, selectors):
    username = account["username"]
    password = account["password"]

    # 1. Setup Context (Desktop Viewport to avoid mobile menu hiding)
    context = await browser.new_context(
        viewport={"width": 1920, "height": 1080},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        locale="en-IN"
    )
    
    # Block heavy media
    await context.route("**/*", lambda route: route.abort() 
        if route.request.resource_type in ["image", "media", "font"] 
        else route.continue_())

    page = await context.new_page()
    await apply_stealth(page)

    try:
        # 2. Navigate
        print(f"[{username}] Navigating...", flush=True)
        try:
            await page.goto(selectors["website"], wait_until="domcontentloaded", timeout=60000)
        except:
            print(f"[{username}] Navigation timed out (continuing)...")

        # 3. Login
        print(f"[{username}] Logging in...", flush=True)
        await dismiss_overlays(page, username)
        
        # Click Login Button (Robust)
        try:
            if await page.locator(selectors["landing_page_login_button"]).is_visible():
                await page.click(selectors["landing_page_login_button"], timeout=5000)
            else:
                # Fallback: Search for visible text if ID is hidden
                await page.click("text=Login >> visible=true", timeout=5000)
        except:
            # JS Fallback
            print(f"[{username}] JS Force Click...")
            await page.evaluate(f"""() => {{
                const btn = document.querySelector('{selectors["landing_page_login_button"]}');
                if (btn) btn.click();
            }}""")

        # 4. Fill Credentials (FORCE MODE)
        try:
            await page.wait_for_selector(selectors["username_field"], state="attached", timeout=10000)
            await page.fill(selectors["username_field"], username, timeout=5000, force=True)
        except:
             await page.evaluate(f"document.querySelector('{selectors['username_field']}').value = '{username}'")
             
        await page.fill(selectors["password_field"], password, force=True)
        await page.press(selectors["password_field"], "Enter")

        # 5. Smart Balance Wait
        print(f"[{username}] Login submitted. Polling for balance...", flush=True)
        
        balance_found = False
        clean_text = "N/A"
        start_time = asyncio.get_event_loop().time()
        
        while (asyncio.get_event_loop().time() - start_time) < 60:
            try:
                await dismiss_overlays(page, username)
                
                bal_loc = page.locator(selectors["avaliable_balance"])
                # Use inner_text to get text even if slightly obscured
                raw_text = await bal_loc.inner_text(timeout=2000)
                
                if raw_text:
                    text = raw_text.strip()
                    if any(c.isdigit() for c in text) and "loading" not in text.lower():
                        clean_text = text
                        print(f"[{username}] SUCCESS: Balance found: {clean_text}", flush=True)
                        balance_found = True
                        break
            except: 
                pass
            await asyncio.sleep(1)

        if balance_found:
             return {
                "username": username,
                "password": password,
                "balance": clean_text,
                "status": "Success",
                "error": "",
            }
        else:
            print(f"[{username}] FAILED: Balance not found.", flush=True)
            os.makedirs(SCREENSHOTS_DIR, exist_ok=True)
            await page.screenshot(path=f"{SCREENSHOTS_DIR}/failed_{username}.png")
            raise Exception("Balance check timed out")

    except Exception as e:
        raise e
    finally:
        await context.close()

# ================= WORKER =================
async def worker(worker_id, queue, browser, selectors):
    while not queue.empty():
        account = await queue.get()
        username = account["username"]
        
        # Random Delay (Important for Anti-Ban)
        delay = random.uniform(2, 8)
        print(f"[Worker {worker_id}] Sleeping {delay:.2f}s...", flush=True)
        await asyncio.sleep(delay)

        success = False
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                print(f"[Worker {worker_id}] {username} | Attempt {attempt}", flush=True)
                result = await process_account(browser, account, selectors)
                
                # Save Success
                await save_result(result)
                async with stats_lock:
                    STATS["processed"] += 1
                    STATS["success"] += 1
                success = True
                break

            except Exception as e:
                err_msg = str(e)
                print(f"[Worker {worker_id}] Error: {err_msg[:100]}")
                
                # Check for critical IP Block
                if "BLOCKED" in err_msg or "403" in err_msg:
                    print("CRITICAL: IP seems blocked. Stopping worker.")
                    # Log Failure
                    fail_data = {"username": username, "password": account["password"], "balance": "0", "status": "Blocked", "error": "IP BLOCKED"}
                    await save_result(fail_data)
                    await save_failed(fail_data) # <--- Save to failed csv
                    async with stats_lock: STATS["failed"] += 1
                    queue.task_done()
                    return

                # If Max Retries reached
                if attempt == MAX_RETRIES:
                    # Log Failure
                    fail_data = {"username": username, "password": account["password"], "balance": "0", "status": "Failed", "error": err_msg}
                    await save_result(fail_data)
                    await save_failed(fail_data) # <--- Save to failed csv
                    async with stats_lock:
                        STATS["processed"] += 1
                        STATS["failed"] += 1
                else:
                    await asyncio.sleep(3) # Wait before retry

        await log_progress()
        queue.task_done()

# ================= MAIN =================
async def main():
    if not os.path.exists(SELECTORS_FILE):
        print("Error: selectors.json missing")
        return

    with open(SELECTORS_FILE) as f: selectors = json.load(f)
    
    # 1. Read Accounts
    if not os.path.exists(ACCOUNTS_FILE):
        print(f"Error: {ACCOUNTS_FILE} missing")
        return
    all_accounts = list(csv.DictReader(open(ACCOUNTS_FILE)))
    
    # 2. Sharding Logic (For Matrix Strategy)
    try:
        shard_index = int(os.getenv("SHARD_INDEX", 0))
        total_shards = int(os.getenv("TOTAL_SHARDS", 1))
    except:
        shard_index = 0
        total_shards = 1
        

    total_accounts = len(all_accounts)
    if total_shards > 1:
        chunk_size = (total_accounts + total_shards - 1) // total_shards
        start_idx = shard_index * chunk_size
        end_idx = min(start_idx + chunk_size, total_accounts)
        my_accounts = all_accounts[start_idx:end_idx]
        print(f"--- SHARD {shard_index + 1}/{total_shards} ---")
        print(f"Processing range: {start_idx} to {end_idx} (Count: {len(my_accounts)})")
    else:
        my_accounts = all_accounts
        print(f"Processing all {len(my_accounts)} accounts")

    if not my_accounts:
        print("No accounts assigned to this shard.")
        return

    STATS["total"] = len(my_accounts)
    queue = asyncio.Queue()
    for acc in my_accounts: queue.put_nowait(acc)

    # 3. Detect CI Environment
    is_ci = os.getenv("GITHUB_ACTIONS") == "true"
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True if is_ci else False,
            args=[
                "--no-sandbox", 
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled"
            ]
        )
        
        workers = [asyncio.create_task(worker(i, queue, browser, selectors)) for i in range(MAX_WORKERS)]
        await asyncio.gather(*workers)
        await browser.close()

    print("All done.", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
