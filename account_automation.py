import asyncio
import csv
import json
import os
import random
from datetime import datetime
from playwright.async_api import async_playwright

# ================= CONFIG =================
MAX_WORKERS = 1  # KEEP THIS AT 1. Parallel = Instant Ban.
MAX_RETRIES = 2
ACCOUNTS_FILE = "accounts.csv"
RESULTS_FILE = "account_balances.csv"
PROGRESS_FILE = "progress.json"
SELECTORS_FILE = "selectors.json"

# ================= GLOBALS =================
csv_lock = asyncio.Lock()
stats_lock = asyncio.Lock()

STATS = {
    "total": 0,
    "processed": 0,
    "success": 0,
    "failed": 0
}

# ================= STEALTH INJECTION =================
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
            get: () => ['en-IN', 'en-GB', 'en-US', 'en'],
        });
    """)

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

async def dismiss_overlays(page):
    try:
        # Click close buttons if they exist
        await page.evaluate("""() => {
            const selectors = ['.modal-close', '.popup-close', 'button[aria-label="Close"]', '.close-btn'];
            selectors.forEach(s => {
                document.querySelectorAll(s).forEach(el => el.click());
            });
        }""")
    except: pass

# ================= CORE LOGIC =================
async def process_account(browser, account, selectors):
    username = account["username"]
    password = account["password"]

    # 1. Setup Context (Indian Location)
    context = await browser.new_context(
        viewport={"width": 1920, "height": 1080},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        locale="en-IN",
        timezone_id="Asia/Kolkata",
        geolocation={"latitude": 17.3850, "longitude": 78.4867},
        permissions=["geolocation"],
        extra_http_headers={"Accept-Language": "en-IN,en;q=0.9"}
    )
    
    # Block media to speed up
    await context.route("**/*", lambda route: route.abort() 
        if route.request.resource_type in ["image", "media", "font"] 
        else route.continue_())

    page = await context.new_page()
    await apply_stealth(page)

    try:
        # 2. Navigate
        print(f"[{username}] Navigating...", flush=True)
        try:
            await page.goto(selectors["website"], wait_until="domcontentloaded", timeout=45000)
        except Exception as e:
            if "Timeout" in str(e):
                title = await page.title()
                if not title: raise Exception("BLOCKED: Timeout & Empty Title")
            raise e

        # 3. Check for Block
        title = await page.title()
        print(f"[{username}] Page Title: {title}", flush=True)
        if not title or title.strip() == "":
            raise Exception("BLOCKED: Generated empty page title.")

        # 4. Login (Improved)
        print(f"[{username}] Logging in...", flush=True)
        await dismiss_overlays(page)
        
        # Click Login Button
        try:
            # Look for ANY button with 'Login' text
            await page.click("text=Login", timeout=5000)
        except:
            # Fallback: JS Click
            await page.evaluate("""() => {
                const els = document.querySelectorAll('a, span, button, div');
                for (const el of els) {
                    if (el.innerText.trim() === 'Login' && el.offsetParent !== null) { el.click(); break; }
                }
            }""")

        # 5. Fill Credentials (ROBUST METHOD)
        # We try specific ID first, then fallbacks if ID is missing (mobile layout)
        try:
            # Try standard ID
            await page.fill(selectors["username_field"], username, timeout=3000)
        except:
            print(f"[{username}] Standard ID not found, searching for inputs...", flush=True)
            # Try Placeholder or Type
            try:
                await page.fill("input[placeholder*='sername']", username, timeout=3000)
            except:
                # Last resort: Fill the first visible text input
                await page.fill("input[type='text']:visible", username, timeout=3000)

        # Fill Password
        try:
            await page.fill(selectors["password_field"], password, timeout=3000)
        except:
            await page.fill("input[type='password']", password, timeout=3000)

        await page.keyboard.press("Enter")

        # 6. Smart Balance Wait (Robust Check)
        print(f"[{username}] Login submitted. Waiting for balance...", flush=True)
        
        start_time = asyncio.get_event_loop().time()
        balance_found = False
        clean_text = "N/A"

        # Poll for 30 seconds
        while (asyncio.get_event_loop().time() - start_time) < 30:
            try:
                # Get text directly (ignoring visibility hidden/mobile drawers)
                raw_text = await page.locator(selectors["avaliable_balance"]).text_content(timeout=500)
                if raw_text:
                    text = raw_text.strip()
                    # Valid if: Not "Loading...", Not Empty, Contains Digit
                    if "loading" not in text.lower() and any(c.isdigit() for c in text):
                        clean_text = text
                        balance_found = True
                        print(f"[{username}] SUCCESS: Balance loaded: {clean_text}", flush=True)
                        break
            except: pass
            await asyncio.sleep(1)

        if balance_found:
            return {"username": username, "password": password, "balance": clean_text, "status": "Success", "error": ""}
        
        # Failure Dump
        print(f"[{username}] FAILED: Balance not found. Dumping HTML...", flush=True)
        os.makedirs("debug_html", exist_ok=True)
        with open(f"debug_html/failed_{username}.html", "w", encoding="utf-8") as f:
            f.write(await page.content())
        raise Exception(f"Balance check timed out. Last read: {clean_text}")

    except Exception as e:
        raise e
    finally:
        await context.close()

# ================= WORKER =================
async def worker(id, queue, browser, selectors):
    while not queue.empty():
        account = await queue.get()
        
        # RANDOM DELAY (Crucial for Anti-Ban)
        delay = random.uniform(10, 20) 
        print(f"[Worker {id}] Sleeping {delay:.2f}s...", flush=True)
        await asyncio.sleep(delay)

        success = False
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                print(f"[Worker {id}] {account['username']} | Attempt {attempt}")
                result = await process_account(browser, account, selectors)
                await save_result(result)
                async with stats_lock: STATS["success"] += 1
                success = True
                break
            except Exception as e:
                err = str(e)
                print(f"[Worker {id}] Error: {err[:100]}...")
                
                # CRITICAL: Stop if blocked to avoid wasting time
                if "BLOCKED" in err:
                    print("CRITICAL: IP seems blocked. Stopping worker.")
                    async with stats_lock: STATS["failed"] += 1
                    await save_result({"username": account['username'], "password": account['password'], "balance": "0", "status": "Failed", "error": "IP BLOCKED"})
                    queue.task_done()
                    return 

                await asyncio.sleep(5)
        
        if not success:
            async with stats_lock: STATS["failed"] += 1
            await save_result({"username": account['username'], "password": account['password'], "balance": "0", "status": "Failed", "error": "Max Retries"})
        
        await log_progress()
        queue.task_done()

# ================= MAIN =================
async def main():
    if not os.path.exists(SELECTORS_FILE):
        print("ERROR: selectors.json missing")
        return

    with open(SELECTORS_FILE) as f: selectors = json.load(f)
    accounts = list(csv.DictReader(open(ACCOUNTS_FILE)))
    STATS["total"] = len(accounts)

    queue = asyncio.Queue()
    for acc in accounts: queue.put_nowait(acc)

    async with async_playwright() as p:
        # Launch with Stealth + Headless New
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--headless=new", # Modern Headless
                "--no-sandbox", 
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars"
            ]
        )
        
        workers = [asyncio.create_task(worker(i, queue, browser, selectors)) for i in range(MAX_WORKERS)]
        await asyncio.gather(*workers)
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
