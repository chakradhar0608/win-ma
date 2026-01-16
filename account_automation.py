import asyncio
import csv
import json
import os
import random
from datetime import datetime
from playwright.async_api import async_playwright

# ================= CONFIG =================
MAX_WORKERS = 1  # Keep 1 for CI to avoid IP bans
MAX_RETRIES = 2

ACCOUNTS_FILE = "accounts.csv"
RESULTS_FILE = "account_balances.csv"
PROGRESS_FILE = "progress.json"
SELECTORS_FILE = "selectors.json"
SCREENSHOTS_DIR = "screenshots"

# ================= GLOBALS =================
csv_lock = asyncio.Lock()
progress_lock = asyncio.Lock()
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

async def save_progress(username):
    async with progress_lock:
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, "r") as f: completed = set(json.load(f))
        else: completed = set()
        
        completed.add(username)
        with open(PROGRESS_FILE, "w") as f: json.dump(sorted(list(completed)), f)

# ================= STEALTH & POPUPS =================
async def apply_stealth(page):
    """Manually hides bot signals for Headless Mode."""
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

async def dismiss_overlays(page, username):
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

    # 1. Setup Context with Desktop Viewport
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

        # 3. Login - ROBUST METHOD
        print(f"[{username}] Logging in...", flush=True)
        await dismiss_overlays(page, username)
        
        # Method A: Try specific button first
        try:
            if await page.locator(selectors["landing_page_login_button"]).is_visible():
                await page.click(selectors["landing_page_login_button"], timeout=5000)
            else:
                raise Exception("Primary button hidden")
        except:
            # Method B: Click the first 'Login' text that is TRULY VISIBLE
            # The '>> visible=true' part is critical here
            print(f"[{username}] Primary button failed. Searching for visible text...")
            try:
                await page.click("text=Login >> visible=true", timeout=5000)
            except:
                # Method C: JS Force Click (Last Resort)
                print(f"[{username}] JS Force Click...")
                await page.evaluate(f"""() => {{
                    const btn = document.querySelector('{selectors["landing_page_login_button"]}');
                    if (btn) btn.click();
                }}""")

        # 4. Fill Credentials (FORCE MODE)
        # We wait for the input to exist, then FORCE fill it to bypass "element not visible" errors
        try:
            await page.wait_for_selector(selectors["username_field"], state="attached", timeout=10000)
            await page.fill(selectors["username_field"], username, timeout=5000, force=True) # <--- FORCE ADDED
        except:
            # Fallback: JS Fill
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
                # Use inner_text as it handles hidden text better
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
            # Take screenshot to see what went wrong
            os.makedirs(SCREENSHOTS_DIR, exist_ok=True)
            await page.screenshot(path=f"{SCREENSHOTS_DIR}/failed_{username}.png")
            raise Exception("Balance check timed out")

    except Exception as e:
        raise e
    finally:
        await context.close()

# ================= WORKER =================
async def worker(worker_id, queue, browser, selectors):
    while True:
        try:
            account = queue.get_nowait()
        except asyncio.QueueEmpty:
            return

        username = account["username"]

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                print(f"[Worker {worker_id}] {username} | Attempt {attempt}", flush=True)
                result = await process_account(browser, account, selectors)
                await save_result(result)
                await save_progress(username)
                
                async with stats_lock:
                    STATS["processed"] += 1
                    STATS["success"] += 1
                await log_progress()
                break

            except Exception as e:
                if attempt == MAX_RETRIES:
                    print(f"[FAILED] {username}: {e}", flush=True)
                    async with stats_lock:
                        STATS["processed"] += 1
                        STATS["failed"] += 1
                    await log_progress()
                else:
                    await asyncio.sleep(2)

        queue.task_done()

# ================= MAIN =================
async def main():
    if not os.path.exists(SELECTORS_FILE):
        print("Error: selectors.json missing")
        return

    with open(SELECTORS_FILE) as f: selectors = json.load(f)
    accounts = list(csv.DictReader(open(ACCOUNTS_FILE)))
    STATS["total"] = len(accounts)

    queue = asyncio.Queue()
    for acc in accounts: queue.put_nowait(acc)

    # DETECT CI ENVIRONMENT
    is_ci = os.getenv("GITHUB_ACTIONS") == "true"
    
    async with async_playwright() as p:
        # Auto-switch Headless based on environment
        browser = await p.chromium.launch(
            headless=True if is_ci else False,  # True in GitHub, False on laptop
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
