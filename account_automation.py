import asyncio
import csv
import json
import os
from datetime import datetime
from playwright.async_api import async_playwright

# ================= CONFIG =================
MAX_WORKERS = 1
MAX_RETRIES = 2

ACCOUNTS_FILE = "accounts.csv"
RESULTS_FILE = "account_balances.csv"
FAILED_FILE = "failed_accounts.csv"
PROGRESS_FILE = "progress.json"
SCREENSHOTS_DIR = "screenshots"
SELECTORS_FILE = "selectors.json"

csv_lock = asyncio.Lock()
failed_lock = asyncio.Lock()
progress_lock = asyncio.Lock()

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

async def save_failed(row):
    """Saves ONLY failed accounts to a separate CSV for easy retrying."""
    async with failed_lock:
        file_exists = os.path.exists(FAILED_FILE)
        with open(FAILED_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, 
                fieldnames=["username", "password", "error", "timestamp"]
            )
            if not file_exists:
                writer.writeheader()
            
            failed_row = {
                "username": row["username"],
                "password": row["password"],
                "error": row["error"],
                "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            }
            writer.writerow(failed_row)

# ================= AGGRESSIVE POPUP REMOVAL =================
async def nuclear_popup_removal(page, username):
    """
    NUCLEAR option: Remove ALL overlays, modals, and high z-index elements.
    """
    try:
        if page.is_closed(): return
        removed_count = await page.evaluate("""() => {
            let count = 0;
            const overlayIds = [
                'strEchApp_ovrlay', 'aviatrix-container_overlay', 'mainPopupWrpr',
                'popup-overlay', 'modal-overlay', 'app-download-popup', 'switchuser_riv',
                'modal', 'popup', 'overlay', 'dialog'
            ];
            overlayIds.forEach(id => {
                const el = document.getElementById(id);
                if (el) { el.remove(); count++; }
            });
            const overlayClasses = [
                'modal-backdrop', 'overlay', 'popup-container', 'modal', 'popup',
                'dialog', 'fade', 'show', 'aviatrix-container_overlay', 
                'instamatch-container_overlay', 'mainPopupWrpr', 'switchuser_riv'
            ];
            overlayClasses.forEach(cls => {
                const els = document.getElementsByClassName(cls);
                while(els.length > 0) { els[0].remove(); count++; }
            });
            const allElements = document.querySelectorAll('*');
            allElements.forEach(el => {
                const zIndex = parseInt(window.getComputedStyle(el).zIndex);
                if (zIndex > 9000) {
                    const rect = el.getBoundingClientRect();
                    if (rect.width > 500 && rect.height > 300) { el.remove(); count++; }
                }
            });
            document.querySelectorAll('div').forEach(div => {
                const style = window.getComputedStyle(div);
                const position = style.position;
                if ((position === 'fixed' || position === 'absolute') && style.display !== 'none') {
                    const rect = div.getBoundingClientRect();
                    const opacity = parseFloat(style.opacity);
                    const bgColor = style.backgroundColor;
                    if (rect.width > window.innerWidth * 0.8 && rect.height > window.innerHeight * 0.5) {
                        if (opacity < 1 || bgColor.includes('rgba') || bgColor === 'rgb(0, 0, 0)') {
                            div.remove(); count++;
                        }
                    }
                }
            });
            document.body.style.overflow = 'auto';
            return count;
        }""")
        if removed_count > 0:
            print(f"[{username}] 🧹 Removed {removed_count} overlay elements", flush=True)
    except Exception as e:
        print(f"[{username}] Nuclear removal error: {str(e)[:50]}", flush=True)

async def click_all_close_buttons(page, username):
    close_selectors = [
        "button[aria-label='Close']", "button[title='Close']", "[class*='close']",
        "[class*='Close']", "[id*='close']", "button.close", "button.btn-close",
        ".modal-close", ".popup-close", "button.animCLseBtn", "button.mnPopupClose",
        "button.pgSoftClsBtn", ".animCLseBtn", ".mnPopupClose", ".pgSoftClsBtn",
        "button:has-text('×')", "button:has-text('X')", "button:has-text('Close')",
        ".modal button", ".popup button", ".overlay button"
    ]
    clicked = 0
    for selector in close_selectors:
        try:
            elements = await page.locator(selector).all()
            for el in elements[:3]:
                try:
                    if await el.is_visible():
                        await el.click(timeout=300, force=True)
                        clicked += 1
                        await asyncio.sleep(0.1)
                except: pass
        except: pass
    if clicked > 0:
        print(f"[{username}] 🎯 Clicked {clicked} close buttons", flush=True)

async def aggressive_popup_cleanup(page, username, rounds=3):
    try:
        for round_num in range(rounds):
            if page.is_closed(): return
            await nuclear_popup_removal(page, username)
            await asyncio.sleep(0.3)
            await click_all_close_buttons(page, username)
            await asyncio.sleep(0.3)
    except:
        pass

# ================= PAGE LOAD DETECTION =================
async def wait_for_page_fully_loaded(page, username, timeout_seconds=30):
    print(f"[{username}] ⏳ Waiting for page to fully load...", flush=True)
    max_checks = timeout_seconds * 2
    stable_count = 0
    required_stable_checks = 6
    
    for check in range(max_checks):
        try:
            is_ready = await page.evaluate("""() => {
                if (document.readyState !== 'complete') return false;
                if (typeof jQuery !== 'undefined' && jQuery(':animated').length > 0) return false;
                const loadingElements = document.querySelectorAll('[class*="loading"], [class*="spinner"], [id*="loading"], [id*="spinner"]');
                for (let el of loadingElements) {
                    const style = window.getComputedStyle(el);
                    if (style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0') return false;
                }
                return true;
            }""")
            if is_ready:
                stable_count += 1
                if stable_count >= required_stable_checks:
                    print(f"[{username}] ✅ Page fully loaded", flush=True)
                    return True
            else:
                stable_count = 0
            await asyncio.sleep(0.5)
        except Exception as e:
            await asyncio.sleep(0.5)
    print(f"[{username}] ⚠️ Load check timeout - proceeding", flush=True)
    return False

# ================= CORE LOGIC =================
async def process_account(browser, account, selectors):
    username = account["username"]
    password = account["password"]

    context = await browser.new_context(
        viewport={"width": 1920, "height": 1080},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        java_script_enabled=True,
        locale="en-IN",
        extra_http_headers={
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
        },
        timezone_id="Asia/Kolkata",
        geolocation={"latitude": 17.3850, "longitude": 78.4867},
        permissions=["geolocation"]
    )
    
    stealth_js = """
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
        window.navigator.chrome = { runtime: {} };
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ? Promise.resolve({ state: 'denied' }) : originalQuery(parameters)
        );
    """
    await context.add_init_script(stealth_js)
    page = await context.new_page()

    try:
        await page.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in ["image", "media", "font", "stylesheet", "other"]
            or any(x in route.request.url for x in ["google-analytics", "facebook", "gtm.js", "fbevents", "ads", "tracker"])
            else route.continue_(),
        )

        print(f"[{username}] 🌐 Navigating...", flush=True)
        try:
            await page.goto(selectors["website"], timeout=90000)
        except:
            print(f"[{username}] ⚠️ Navigation timed out (continuing)...", flush=True)

        # Check for 403 BLOCKED
        try:
            title = await page.title()
            print(f"[{username}] Page title: {title}", flush=True)
            content = await page.content()
            if "403 Forbidden" in title or "Access Denied" in content or "403 Forbidden" in content:
                print(f"[{username}] ⛔ 403 BLOCKED DETECTED! Aborting.", flush=True)
                raise Exception("BLOCKED: 403 Forbidden")
        except Exception as e:
            if "BLOCKED" in str(e): raise e
        
        await asyncio.sleep(2)
        await aggressive_popup_cleanup(page, username, rounds=2)

        print(f"[{username}] 🔑 Clicking landing page login...", flush=True)
        try:
            await page.click(selectors["landing_page_login_button"], timeout=60000)
        except:
            try: 
                await page.evaluate(f"document.querySelector('{selectors['landing_page_login_button']}').click()")
            except: 
                print(f"[{username}] ⚠️ Landing page login button click failed", flush=True)
        
        await asyncio.sleep(1)

        # ===== IMPROVED LOGIN SUBMISSION =====
        print(f"[{username}] ✍️ Filling credentials...", flush=True)
        await page.fill(selectors["username_field"], username, timeout=60000)
        await page.fill(selectors["password_field"], password, timeout=60000)
        
        print(f"[{username}] 🔐 Submitting login...", flush=True)
        submission_success = False
        
        # Method 1: Click #loginbutton (most reliable)
        try:
            await page.click("#loginbutton", timeout=5000)
            submission_success = True
            print(f"[{username}] ✓ Clicked login button", flush=True)
        except Exception as e:
            print(f"[{username}] ⚠️ Login button click failed: {str(e)[:50]}", flush=True)
            
            # Fallback 1: Force click
            try:
                await page.locator("#loginbutton").click(force=True, timeout=3000)
                submission_success = True
                print(f"[{username}] ✓ Force-clicked login button", flush=True)
            except:
                pass
        
        # Method 2: Press Enter on password field (backup)
        if not submission_success:
            try:
                await page.press(selectors["password_field"], "Enter")
                print(f"[{username}] ✓ Pressed Enter", flush=True)
                submission_success = True
            except Exception as e:
                print(f"[{username}] ⚠️ Enter press failed: {str(e)[:50]}", flush=True)
        
        # Method 3: JavaScript form submission (last resort)
        if not submission_success:
            try:
                await page.evaluate("""
                    const loginBtn = document.querySelector('#loginbutton');
                    if (loginBtn) {
                        loginBtn.click();
                    } else {
                        const form = document.querySelector('form');
                        if (form) form.submit();
                        else {
                            const submitBtn = document.querySelector('button[type="submit"]');
                            if (submitBtn) submitBtn.click();
                        }
                    }
                """)
                print(f"[{username}] ✓ JS form submit", flush=True)
            except Exception as e:
                print(f"[{username}] ⚠️ JS submit failed: {str(e)[:50]}", flush=True)
        
        print(f"[{username}] ⏳ Waiting for login redirect...", flush=True)
        
        # Wait for URL change or network idle
        otp = False
        try:
            # Wait for navigation to complete (max 120 seconds)
            await page.wait_for_url("**/?uid=*", timeout=120000)
            otp = True
            print(f"[{username}] 🚀 Redirected successfully!", flush=True)
        except Exception as e:
            # Fallback: Check URL periodically (reduced to 60 iterations)
            print(f"[{username}] ⏳ URL pattern not detected, checking manually...", flush=True)
            for i in range(60):
                current_url = page.url
                print(f"[{username}] Attempt {i+1}/60 - URL: {current_url}", flush=True)
                
                if "?uid=" in current_url:
                    otp = True
                    print(f"[{username}] 🚀 Redirected successfully!", flush=True)
                    break
                
                # Check for login errors
                try:
                    error_selectors = [
                        "text=Invalid credentials",
                        "text=Wrong password",
                        "text=Login failed",
                        ".error-message",
                        ".login-error"
                    ]
                    for err_sel in error_selectors:
                        if await page.locator(err_sel).is_visible(timeout=100):
                            raise Exception("Login credentials rejected")
                except:
                    pass
                
                await asyncio.sleep(1)
        
        if not otp:
            raise Exception("Login failed - no redirect after 60 seconds")

        print(f"[{username}] 🧹 Post-login cleanup...", flush=True)
        await aggressive_popup_cleanup(page, username, rounds=4)
        await wait_for_page_fully_loaded(page, username)

        print(f"[{username}] 💰 Looking for balance...", flush=True)
        balance = "N/A"
        
        for attempt in range(100):
            try:
                if attempt % 2 == 0:
                    await nuclear_popup_removal(page, username)
                    await click_all_close_buttons(page, username)
                
                bal_loc = page.locator(selectors["avaliable_balance"])
                if await bal_loc.is_visible():
                    text = (await bal_loc.inner_text(timeout=10000)).strip()
                    if text and any(c.isdigit() for c in text) and "LOADING" not in text.upper():
                        balance = text
                        print(f"[{username}] ✅ Balance found: {balance}", flush=True)
                        break
            except: 
                pass
            await asyncio.sleep(1.5)
        
        os.makedirs(SCREENSHOTS_DIR, exist_ok=True)
        await page.screenshot(path=os.path.join(SCREENSHOTS_DIR, f"{username}.png"), full_page=True)

        if balance != "N/A":
            return { "username": username, "password": password, "balance": balance, "status": "Success", "error": "" }
        else:
            raise Exception("Balance not found - popups may be blocking view")

    except Exception as e:
        print(f"[{username}] ❌ Error: {str(e)[:100]}", flush=True)
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
                print(f"[Worker {worker_id}] 🚀 Processing: {username} (Attempt {attempt})", flush=True)
                result = await process_account(browser, account, selectors)

                if result["balance"]:
                    await save_result(result)
                    await save_progress(username)
                    print(f"[Worker {worker_id}] ✅ SUCCESS: {username}", flush=True)
                    break

            except Exception as e:
                if attempt == MAX_RETRIES:
                    print(f"[Worker {worker_id}] ❌ FAILED: {username}", flush=True)
                    
                    fail_data = {
                        "username": username,
                        "password": account["password"],
                        "balance": "N/A",
                        "status": "Failed",
                        "error": str(e),
                    }
                    await save_result(fail_data)
                    await save_failed(fail_data)

                    try:
                        ctx = await browser.new_context()
                        pg = await ctx.new_page()
                        await pg.goto(selectors["website"], timeout=30000)
                        await pg.screenshot(path=os.path.join(SCREENSHOTS_DIR, f"{username}_ERROR.png"))
                        await ctx.close()
                    except: 
                        pass
                else:
                    await asyncio.sleep(2)
        queue.task_done()

# ================= MAIN =================
async def main():
    os.makedirs(SCREENSHOTS_DIR, exist_ok=True)
    if not os.path.exists(SELECTORS_FILE):
        print("Error: selectors.json missing")
        return
    with open(SELECTORS_FILE, "r", encoding="utf-8") as f:
        selectors = json.load(f)

    # READ ACCOUNTS
    all_accounts = []
    if not os.path.exists(ACCOUNTS_FILE):
        print(f"Error: {ACCOUNTS_FILE} not found.")
        return
    with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["username"]:
                all_accounts.append(row)

    # SHARDING LOGIC
    try:
        shard_index = int(os.getenv("SHARD_INDEX", 0))
        total_shards = int(os.getenv("TOTAL_SHARDS", 1))
    except:
        shard_index = 0
        total_shards = 1

    total_len = len(all_accounts)
    if total_shards > 1:
        chunk_size = (total_len + total_shards - 1) // total_shards
        start_idx = shard_index * chunk_size
        end_idx = min(start_idx + chunk_size, total_len)
        my_accounts_raw = all_accounts[start_idx:end_idx]
        print(f"--- SHARD {shard_index + 1}/{total_shards} ---")
        print(f"Processing range: {start_idx} to {end_idx} (Count: {len(my_accounts_raw)})")
    else:
        my_accounts_raw = all_accounts
        print(f"Processing all {len(my_accounts_raw)} accounts")

    # FILTER COMPLETED
    completed = load_progress()
    accounts = [acc for acc in my_accounts_raw if acc["username"] not in completed]

    if not accounts:
        print("Nothing left to process in this shard.")
        return

    queue = asyncio.Queue()
    for acc in accounts: 
        queue.put_nowait(acc)

    is_ci = os.getenv("GITHUB_ACTIONS") == "true"
    use_headless = True if is_ci else False

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=use_headless,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"],
        )
        workers = [asyncio.create_task(worker(i, queue, browser, selectors)) for i in range(MAX_WORKERS)]
        await asyncio.gather(*workers)
        await browser.close()

    print("🎉 All done!")

if __name__ == "__main__":
    asyncio.run(main())
