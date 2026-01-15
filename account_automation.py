import asyncio
import csv
import json
import os
from datetime import datetime
from playwright.async_api import async_playwright

# ================= CONFIG =================
MAX_WORKERS = 10
MAX_RETRIES = 5

ACCOUNTS_FILE = "instamatch_passwords.csv"
RESULTS_FILE = "account_balances.csv"
PROGRESS_FILE = "progress.json"
SCREENSHOTS_DIR = "screenshots"
SELECTORS_FILE = "selectors.json"

csv_lock = asyncio.Lock()
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

# ================= CORE LOGIC =================
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
        # Specifics from user code
        "#app-next > div.mb-app > div.aviatrix-container_overlay:nth-of-type(22) > div.aviatrix-container > button.animCLseBtn:nth-of-type(1) > span",
        "#app-next > div.mb-app:nth-of-type(1) > div.mainPopupWrpr.mainPopupWrpr_pgsoft:nth-of-type(20) > div.mnPopupCtntPar > button.mnPopupBtn.mnPopupClose.pgSoftClsBtn:nth-of-type(2)"
    ]

    for selector in close_patterns:
        try:
            # Try to click if visible, short timeout
            loc = page.locator(selector).first
            if await loc.is_visible():
                await loc.click(timeout=500)
                # print(f"[{username}] Closed popup: {selector}")
                await asyncio.sleep(0.5)
        except:
            pass

async def process_account(browser, account, selectors):
    username = account["username"]
    password = account["password"]

    context = await browser.new_context(
        viewport={"width": 1920, "height": 1080},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        java_script_enabled=True,
        locale="en-US",
    )
    await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    page = await context.new_page()

    try:
        # Speed optimization
        await page.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in ["image", "media", "font"]
            else route.continue_(),
        )

        # 1. Navigate
        await page.goto(selectors["website"], wait_until="domcontentloaded",timeout=600000)
        await asyncio.sleep(2)
        
        # 2. Cleanup before login click
        await dismiss_overlays(page, username)

        # 3. Click Login Button
        try:
            await page.click(selectors["landing_page_login_button"], timeout=10000)
        except:
            # Fallback JS click
            try:
                await page.evaluate(f"document.querySelector('{selectors['landing_page_login_button']}').click()")
            except:
                pass
        
        await asyncio.sleep(1)
        await dismiss_overlays(page, username)

        # 4. Fill Credentials
        await page.fill(selectors["username_field"], username)
        await page.fill(selectors["password_field"], password)
        await page.press(selectors["password_field"], "Enter")

        # 5. Wait for Login & Cleanup
        try:
            await page.wait_for_load_state("networkidle", timeout=10000)
        except:
            pass
            
        await asyncio.sleep(2)
        await dismiss_overlays(page, username) # Aggressive clean after login

        # 6. Extract Balance
        balance = "N/A"
        try:
            bal_loc = page.locator(selectors["avaliable_balance"])
            await bal_loc.wait_for(state="visible", timeout=30000)
            
            # Retry logic: Wait for actual number (not "LOADING...", "...", or empty)
            # Retries up to 10 times (approx 10-15 seconds)
            for _ in range(10):
                text = (await bal_loc.inner_text()).strip()
                
                # Check if it has any digits (is a number-like string)
                has_digits = any(char.isdigit() for char in text)
                
                if text and has_digits and "LOADING" not in text.upper() and "..." not in text:
                    balance = text
                    print("balance :" ,balance)
                    break
                
                # If still loading, wait and try clearing popups again in case they obscure it
                await asyncio.sleep(1.5)
                await dismiss_overlays(page, username)
        except:
            pass

        os.makedirs(SCREENSHOTS_DIR, exist_ok=True)
        await page.screenshot(
            path=os.path.join(SCREENSHOTS_DIR, f"{username}.png"),
            full_page=True,
        )

        if balance != "N/A":
            print("No balance")
            return {
                "username": username,
                "password": password,
                "balance": balance,
                "status": "Success",
                "error": "",
            }
        else:
             raise Exception("Balance not found or loading")

    except Exception as e:
        # Final cleanup attempt before error screenshot
        await dismiss_overlays(page, username)
        raise e # Re-raise to be caught by worker loop

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
                print("Procsseing account : ",account)
                result = await process_account(browser, account, selectors)

                if result["balance"]:
                    await save_result(result)
                    await save_progress(username)
                    break
                else:
                    raise Exception("Empty balance")

            except Exception as e:
                if attempt == MAX_RETRIES:
                    await save_result({
                        "username": username,
                        "password": account["password"],
                        "balance": "N/A",
                        "status": "Failed",
                        "error": str(e),
                    })

                    try:
                        ctx = await browser.new_context()
                        pg = await ctx.new_page()
                        await pg.goto(selectors["website"], timeout=30000)
                        await pg.screenshot(
                            path=os.path.join(SCREENSHOTS_DIR, f"{username}_ERROR.png")
                        )
                        await ctx.close()
                    except:
                        pass

                else:
                    await asyncio.sleep(2)

        queue.task_done()

# ================= MAIN =================
async def main():
    os.makedirs(SCREENSHOTS_DIR, exist_ok=True)

    with open(SELECTORS_FILE, "r", encoding="utf-8") as f:
        selectors = json.load(f)

    completed = load_progress()

    accounts = []
    if not os.path.exists(ACCOUNTS_FILE):
        print(f"Error: {ACCOUNTS_FILE} not found.")
        return

    with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["username"] and row["username"] not in completed:
                accounts.append(row)

    if not accounts:
        print("Nothing left to process.")
        return

    queue = asyncio.Queue()
    for acc in accounts:
        queue.put_nowait(acc)

    # Force headless in CI (GitHub Actions)
    is_ci = os.getenv("GITHUB_ACTIONS") == "true"
    use_headless = True if is_ci else False  # User defaults to False locally

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=use_headless,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )

        workers = [
            asyncio.create_task(worker(i, queue, browser, selectors))
            for i in range(MAX_WORKERS)
        ]

        await asyncio.gather(*workers)
        await browser.close()

    print("All done.")

if __name__ == "__main__":
    asyncio.run(main())
