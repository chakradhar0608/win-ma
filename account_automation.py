import asyncio
import csv
import json
import os
import random
from datetime import datetime
from playwright.async_api import async_playwright

# ================= CONFIG =================
MAX_WORKERS = 1  # Keep 1 to prevent IP bans
MAX_RETRIES = 2

ACCOUNTS_FILE = "accounts.csv"
RESULTS_FILE = "account_balances.csv"
FAILED_FILE = "failed_accounts.csv"
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
    async with csv_lock:
        file_exists = os.path.exists(RESULTS_FILE)
        with open(RESULTS_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["username", "password", "balance", "status", "error", "timestamp"])
            if not file_exists: writer.writeheader()
            row["timestamp"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            writer.writerow(row)

async def save_failed(row):
    async with failed_lock:
        file_exists = os.path.exists(FAILED_FILE)
        with open(FAILED_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["username", "password", "error", "timestamp"])
            if not file_exists: writer.writeheader()
            failed_row = {
                "username": row["username"],
                "password": row["password"],
                "error": row["error"],
                "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            }
            writer.writerow(failed_row)

# ================= STEALTH & POPUPS =================
async def apply_stealth(page):
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
    # 1. Escape Key
    try: await page.keyboard.press("Escape")
    except: pass

    # 2. JS Removal
    try:
        await page.evaluate("""() => {
            const ids = ["strEchApp_ovrlay", "aviatrix-container_overlay", "mainPopupWrpr", "popup-overlay", "modal-overlay", "app-download-popup", "switchuser_riv"];
            ids.forEach(id => { const el = document.getElementById(id); if (el) el.remove(); });
            const classes = ["modal-backdrop", "fade", "show", "overlay", "popup-container", "modal-content"];
            classes.forEach(cls => { 
                const els = document.getElementsByClassName(cls); 
                for(let i=0; i<els.length; i++) {
                    if (window.getComputedStyle(els[i]).zIndex > 999) els[i].remove();
                } 
            });
        }""")
    except: pass

    # 3. Click Close Buttons
    close_patterns = [
        "button.animCLseBtn", "button.mnPopupClose", ".popup-close", ".modal-close",
        "button[aria-label='Close']", "[class*='close']", ".close-btn", ".pgSoftClsBtn",
        ".close", "i.fa-times", "svg[data-icon='close']", ".modal-header button"
    ]
    for selector in close_patterns:
        try:
            if await page.locator(selector).first.is_visible():
                await page.locator(selector).first.click(timeout=500)
                await asyncio.sleep(0.2)
        except: pass

    # 4. Handle "Download App" Popup
    try:
        if await page.locator("text=DOWNLOAD THE APP NOW").is_visible():
            await page.locator("button.close, .close-icon").first.click(timeout=1000)
    except: pass

# ================= CORE LOGIC =================
async def process_account(browser, account, selectors):
    username = account["username"]
    password = account["password"]

    # 1. Setup Context
    context = await browser.new_context(
        viewport={"width": 1920, "height": 1080},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        locale="en-IN"
    )
    
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

        # 3. Login with 30x Retry Loop
        print(f"[{username}] Logging in...", flush=True)
        login_successful = False
        
        # Retry loop for finding the login button
        for i in range(30):
            await dismiss_overlays(page, username)
            try:
                # Priority 1: Standard Button
                if await page.locator(selectors["landing_page_login_button"]).is_visible():
                    await page.click(selectors["landing_page_login_button"], timeout=5000)
                    login_successful = True
                    break
                # Priority 2: Text Match
                elif await page.locator("text=Login >> visible=true").is_visible():
                    await page.click("text=Login >> visible=true", timeout=5000)
                    login_successful = True
                    break
            except:
                pass # Ignore errors and retry
            
            # Wait 1s before retrying
            await asyncio.sleep(1)

        # Fallback: JS Force Click if loop failed
        if not login_successful:
            print(f"[{username}] Standard click failed. Attempting JS Force Click...", flush=True)
            try:
                await page.evaluate(f"""() => {{
                    const btn = document.querySelector('{selectors["landing_page_login_button"]}');
                    if (btn) btn.click();
                }}""")
            except: pass

        # 4. Fill Credentials (FORCE MODE)
        try:
            await page.wait_for_selector(selectors["username_field"], state="attached", timeout=10000)
            await page.fill(selectors["username_field"], username, timeout=50000, force=True)
        except:
             await page.evaluate(f"document.querySelector('{selectors['username_field']}').value = '{username}'")
             
        await page.fill(selectors["password_field"], password, force=True)
        await page.press(selectors["password_field"], "Enter")

        # 5. Smart Balance Wait (240s Timeout)
        print(f"[{username}] Login submitted. Polling for balance...", flush=True)
        
        balance_found = False
        clean_text = "N/A"
        start_time = asyncio.get_event_loop().time()
        
        while (asyncio.get_event_loop().time() - start_time) < 240:
            try:
                await dismiss_overlays(page, username)
                await asyncio.sleep(1) # Small pause for animations
                
                bal_loc = page.locator(selectors["avaliable_balance"])
                raw_text = await bal_loc.inner_text(timeout=20000)
                
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
        
        delay = random.uniform(2, 5)
        print(f"[Worker {worker_id}] Sleeping {delay:.2f}s...", flush=True)
        await asyncio.sleep(delay)

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                print(f"[Worker {worker_id}] {username} | Attempt {attempt}", flush=True)
                result = await process_account(browser, account, selectors)
                
                await save_result(result)
                async with stats_lock:
                    STATS["processed"] += 1
                    STATS["success"] += 1
                break

            except Exception as e:
                err_msg = str(e)
                if "BLOCKED" in err_msg or "403" in err_msg:
                    print("CRITICAL: IP seems blocked. Stopping worker.")
                    fail_data = {"username": username, "password": account["password"], "balance": "0", "status": "Blocked", "error": "IP BLOCKED"}
                    await save_result(fail_data)
                    await save_failed(fail_data)
                    async with stats_lock: STATS["failed"] += 1
                    queue.task_done()
                    return

                if attempt == MAX_RETRIES:
                    fail_data = {"username": username, "password": account["password"], "balance": "0", "status": "Failed", "error": err_msg}
                    await save_result(fail_data)
                    await save_failed(fail_data)
                    async with stats_lock:
                        STATS["processed"] += 1
                        STATS["failed"] += 1
                else:
                    await asyncio.sleep(3)

        await log_progress()
        queue.task_done()

# ================= MAIN =================
async def main():
    if not os.path.exists(SELECTORS_FILE):
        print("Error: selectors.json missing")
        return

    with open(SELECTORS_FILE) as f: selectors = json.load(f)
    
    if not os.path.exists(ACCOUNTS_FILE):
        print(f"Error: {ACCOUNTS_FILE} missing")
        return
    all_accounts = list(csv.DictReader(open(ACCOUNTS_FILE)))
    
    # Sharding Logic
    try:
        shard_index = int(os.getenv("SHARD_INDEX", 0))
        total_shards = int(os.getenv("TOTAL_SHARDS", 1))
    except:
        shard_index, total_shards = 0, 1

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
