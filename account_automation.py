import asyncio
import csv
import json
import os
from datetime import datetime
from playwright.async_api import async_playwright

# ================= CONFIG =================
MAX_WORKERS = 5
MAX_RETRIES = 20

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
            await page.evaluate(f"""
                () => {{
                    document.getElementById('{identifier}')?.remove();
                    [...document.getElementsByClassName('{identifier}')].forEach(e => e.remove());
                }}
            """)
        except:
            pass

# ================= CORE =================
async def process_account(browser, account, selectors):
    username = account["username"]
    password = account["password"]

    context = await browser.new_context(viewport={"width": 1920, "height": 1080})
    page = await context.new_page()

    try:
        await page.goto(selectors["website"], wait_until="domcontentloaded", timeout=6000000)
        await dismiss_overlays(page, username)

        await page.click(selectors["landing_page_login_button"], timeout=6000000)
        await page.fill(selectors["username_field"], username)
        await page.fill(selectors["password_field"], password)
        await page.press(selectors["password_field"], "Enter")

        await page.wait_for_load_state("networkidle", timeout=6000000)

        bal_loc = page.locator(selectors["avaliable_balance"])
        await bal_loc.wait_for(state="visible", timeout=6000000)

        for _ in range(10):
            text = (await bal_loc.inner_text()).strip()
            if any(c.isdigit() for c in text):
                print(f"[{username}] Balance detected: {text}", flush=True)
                return {
                    "username": username,
                    "password": password,
                    "balance": text,
                    "status": "Success",
                    "error": "",
                }
            await asyncio.sleep(1.5)

        raise Exception("Balance not found")

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
    with open(SELECTORS_FILE, "r", encoding="utf-8") as f:
        selectors = json.load(f)

    accounts = list(csv.DictReader(open(ACCOUNTS_FILE)))
    STATS["total"] = len(accounts)

    print(f"Starting {STATS['total']} accounts", flush=True)

    queue = asyncio.Queue()
    for acc in accounts:
        queue.put_nowait(acc)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        tasks = [asyncio.create_task(worker(i, queue, browser, selectors)) for i in range(MAX_WORKERS)]
        await asyncio.gather(*tasks)
        await browser.close()

    print("All done.", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
