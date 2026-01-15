import asyncio
import csv
import json
import random
import os
from datetime import datetime
from playwright.async_api import async_playwright

# ================= CONFIG =================
MAX_WORKERS = 3
MAX_RETRIES = 3

ACCOUNTS_FILE = "accounts.csv"
RESULTS_FILE = "account_balances.csv"
PROGRESS_FILE = "progress.json"
SCREENSHOTS_DIR = "screenshots"
SELECTORS_FILE = "selectors.json"

csv_lock = asyncio.Lock()
progress_lock = asyncio.Lock()
stats_lock = asyncio.Lock()

STATS = {
    "total": 0,
    "processed": 0,
    "success": 0,
    "failed": 0,
    "balance_gt_1": []
}

def parse_balance(balance_str):
    try:
        clean = "".join(c for c in balance_str if c.isdigit() or c == '.')
        return float(clean)
    except:
        return 0.0

async def log_progress():
    async with stats_lock:
        print(
            f"\n[PROGRESS] {STATS['processed']}/{STATS['total']} | "
            f"Success: {STATS['success']} | Failed: {STATS['failed']} | "
            f"High Balances: {STATS['balance_gt_1']}",
            flush=True
        )

# ================= PROGRESS =================
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    return set()

async def save_progress(username):
    async with progress_lock:
        completed = load_progress()
        completed.add(username)
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(list(completed)), f, indent=2)

# ================= CSV SAVE =================
async def save_result(row):
    async with csv_lock:
        file_exists = os.path.exists(RESULTS_FILE)
        with open(RESULTS_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["username", "password", "balance", "status", "error", "timestamp"]
            )
            if not file_exists:
                writer.writeheader()

            row["timestamp"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            writer.writerow(row)

# ================= POPUP HANDLING =================
async def dismiss_overlays(page, username):
    """
    Dismiss overlays that may block clicks using JavaScript and known selectors.
    """
    # Overlays to remove via JS
    overlay_identifiers = [
        "strEchApp_ovrlay",
        "aviatrix-container_overlay",
        "mainPopupWrpr",
        "popup-overlay",
        "modal-overlay",
        "app-download-popup",
        "switchuser_riv"
    ]

    for identifier in overlay_identifiers:
        try:
            await page.evaluate(f"""() => {{
                // Remove by Class
                const els = document.getElementsByClassName('{identifier}');
                for (let i = els.length - 1; i >= 0; i--) els[i].remove();
                
                // Remove by ID
                const el = document.getElementById('{identifier}');
                if (el) el.remove();
            }}""")
        except:
            pass

    # Aggressive close button clicking
    close_patterns = [
        "button.animCLseBtn",
        "button.mnPopupClose",
        ".popup-close",
        ".modal-close",
        "button[aria-label='Close']",
        "[class*='close']",
        # Specifics
        "#app-next > div.mb-app > div.aviatrix-container_overlay:nth-of-type(22) > div.aviatrix-container > button.animCLseBtn:nth-of-type(1) > span",
        "#app-next > div.mb-app:nth-of-type(1) > div.mainPopupWrpr.mainPopupWrpr_pgsoft:nth-of-type(20) > div.mnPopupCtntPar > button.mnPopupBtn.mnPopupClose.pgSoftClsBtn:nth-of-type(2)"
    ]

    for selector in close_patterns:
        try:
            # Try to click if visible, short timeout
            loc = page.locator(selector).first
            if await loc.is_visible():
                await loc.click(timeout=2000)
                await asyncio.sleep(0.5)
        except:
            pass

# ================= CORE =================
async def process_account(browser, account, selectors):
    username = account["username"]
    password = account["password"]

    # 1. Setup Context with Real User Agent (Bypasses basic bot detection)
    context = await browser.new_context(
        viewport={"width": 1920, "height": 1080},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        java_script_enabled=True,
        locale="en-IN",              # 1. Set Locale to India
        timezone_id="Asia/Kolkata",  # 2. Set Timezone to IST
        geolocation={"latitude": 17.3850, "longitude": 78.4867}, # 3. Set GPS (Hyderabad)
        permissions=["geolocation"]  # Allow site to see the fake location
    )
    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
    """)
    
    # Block heavy resources to speed up loading
    async def block_resources(route):
        if route.request.resource_type in ["image", "media", "font"]:
            await route.abort()
        else:
            await route.continue_()
            
    await context.route("**/*", block_resources)
    page = await context.new_page()

    try:
        # 2. Navigate
        print(f"[{username}] Navigating...", flush=True)
        # Reduced timeout to 60s so it doesn't hang forever
        await page.goto(selectors["website"], wait_until="domcontentloaded", timeout=60000)
        title = await page.title()
        print(f"[{username}] Page Title: {title}", flush=True)
        if not title:
            # If title is empty, we are blocked. Throw error to trigger retry.
            raise Exception("BLOCKED: Generated empty page title.")
        # 3. Login
        print(f"[{username}] Logging in...", flush=True)
        # Attempt to dismiss pre-login popups
        await dismiss_overlays(page, username)
        
        try:
            # OPTION 1: Try the specific class we saw in your logs, but ensure it's visible
            # The logs showed the class "cls_login_btn" or "cls_login_content"
            await page.click("a.loginBtn:visible, .cls_login_btn:visible, span:text('Login'):visible", timeout=5000)
        except:
            print(f"[{username}] Primary login buttons not found/visible. Trying fallbacks...")
            
            # OPTION 2: Force click using JavaScript (bypasses visibility checks)
            # This is dangerous but effective for "hidden" buttons
            found = await page.evaluate("""() => {
                const els = document.querySelectorAll('a, button, span, div');
                for (const el of els) {
                    if (el.innerText.trim() === 'Login' && el.offsetParent !== null) {
                        el.click();
                        return true;
                    }
                }
                return false;
            }""")
            
            if not found:
                raise Exception("Could not find any clickable 'Login' button")
            
        await page.fill(selectors["username_field"], username)
        await page.fill(selectors["password_field"], password)
        await page.press(selectors["password_field"], "Enter")

        # 4. Wait for Page Load (Fixed 5s wait is safer than networkidle here)
        # 4 & 5. OPTIMIZED: Smart Wait for Balance
        print(f"[{username}] Login submitted. Waiting for balance...", flush=True)
        
        start_time = asyncio.get_event_loop().time()
        timeout = 30  # Wait up to 30 seconds
        balance_found = False
        clean_text = "0.0"

        while (asyncio.get_event_loop().time() - start_time) < timeout:
            try:
                # Get the text content (ignoring visibility)
                bal_loc = page.locator(selectors["avaliable_balance"])
                raw_text = await bal_loc.text_content(timeout=1000)
                
                if raw_text:
                    text = raw_text.strip()
                    
                    # === CRITICAL CHECK ===
                    # 1. Must NOT contain "Loading..." (case-insensitive)
                    # 2. Must contain at least one digit (0-9) to be a valid amount
                    if "loading" not in text.lower() and any(c.isdigit() for c in text):
                        # Use the text exactly as found (e.g., "₹ 0.00" or "₹ 120")
                        clean_text = text
                        balance_found = True
                        print(f"[{username}] SUCCESS: Balance loaded: {clean_text}", flush=True)
                        break
            except:
                pass # Ignore errors while page renders

            await asyncio.sleep(0.5)

        if balance_found:
             return {
                "username": username,
                "password": password,
                "balance": clean_text, # Returns "₹ 0.00" or "₹ 120"
                "status": "Success",
                "error": "",
            }

        # 6. Fallback: Dump HTML if we timed out
        print(f"[{username}] FAILED: Stuck on '{clean_text}' or empty. Dumping HTML...", flush=True)
        
        os.makedirs("debug_html", exist_ok=True)
        os.makedirs("screenshots", exist_ok=True)
        await page.screenshot(path=f"screenshots/failed_{username}.png")
        html_content = await page.content()
        with open(f"debug_html/failed_{username}.html", "w", encoding="utf-8") as f:
            f.write(html_content)

        raise Exception(f"Balance not found (Last read: {clean_text})")

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
        delay = (worker_id * 5) + random.uniform(1, 10)
        print(f"[Worker {worker_id}] Sleeping {delay:.2f}s before starting {username}...", flush=True)
        await asyncio.sleep(delay)
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
    with open(SELECTORS_FILE, "r", encoding="utf-8") as f:
        selectors = json.load(f)

    accounts = list(csv.DictReader(open(ACCOUNTS_FILE)))
    STATS["total"] = len(accounts)

    print(f"Starting {STATS['total']} accounts", flush=True)

    queue = asyncio.Queue()
    for acc in accounts:
        queue.put_nowait(acc)

    async with async_playwright() as p:
        
        # UPDATED LAUNCH ARGS
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--headless=new",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage", # Essential for Docker/CI environments
                "--disable-accelerated-2d-canvas",
                "--no-zygote",
                "--start-maximized"
                "--disable-blink-features=AutomationControlled"
            ]
        )
        tasks = [asyncio.create_task(worker(i, queue, browser, selectors)) for i in range(MAX_WORKERS)]
        await asyncio.gather(*tasks)
        await browser.close()

    print("All done.", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
