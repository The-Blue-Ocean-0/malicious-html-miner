import pandas as pd
import asyncio
import time
import json
import os
import re
from playwright.async_api import async_playwright

# ------------------- 설정 -------------------
RESULT_DIR = 'data/results/batch_results'     # バッチ結果フォルダ
EXEC_FILE = 'urls_data.csv'     # 実行ファイル
SCREENSHOT_DIR = 'data/screenshots'           # スクリーンショット保存先
ENABLE_SCREENSHOT = False
BATCH_SIZE =20                           # 1バッチごとのURL数   
MAX_CONCURRENT = 5                            # 同時接続最大数
NONE_DATA = 'null'
# --------------------------------------------

os.makedirs(SCREENSHOT_DIR, exist_ok=True)
os.makedirs(RESULT_DIR, exist_ok=True)
sem = asyncio.Semaphore(MAX_CONCURRENT)

async def extract_head_elements(url, index, total):
    result = {
        "original_url": url,
        "final_url": None,
        "redirect_chain": None,
        "redirect_count": 0,
        "head_elements": None,
        "timeout": False,
        "has_meta_refresh": False,
        "meta_refresh_url": None,
        "duration_sec": None,
    }

    print(f"[{index+1}/{total}] ▶️ Start: {url}")
    start_time = time.time()

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

            redirect_chain = []
            page.on("response", lambda response: (
                redirect_chain.append({
                    "url": response.url,
                    "status": response.status,
                    "location": response.headers.get("location", "")
                }) if response.status in [301, 302, 303, 307, 308] else None
            ))

            try:
                await page.goto(url, timeout=10000, wait_until='domcontentloaded')
                result["final_url"] = page.url
                result["redirect_chain"] = (
                    None if len(redirect_chain) == 0 else json.dumps(redirect_chain, ensure_ascii=False)
                )
                result["redirect_count"] = len(redirect_chain)

                html = await page.content()

                match = re.search(r'<meta[^>]+http-equiv=["\']refresh["\'][^>]+content=["\']\s*\d+\s*;\s*url=([^"\']+)["\']', html, re.IGNORECASE)
                if match:
                    result["has_meta_refresh"] = True
                    result["meta_refresh_url"] = match.group(1).strip()

                elements = []
                head_children = await page.query_selector_all("head > *")
                for tag in head_children:
                    tag_name = await tag.evaluate("(el) => el.tagName.toLowerCase()")
                    if tag_name in ["style", "script"]:
                        continue
                    attributes = await tag.evaluate("(el) => Object.fromEntries([...el.attributes].map(attr => [attr.name, attr.value]))")
                    text = await tag.inner_text()
                    elements.append({
                        "tag": tag_name,
                        "attributes": attributes,
                        "text": text.strip() if text else ""
                    })

                if elements:
                    result["head_elements"] = elements
                else:
                    if ENABLE_SCREENSHOT:
                        await page.screenshot(path=f"{SCREENSHOT_DIR}/{index+1}.png")

            except Exception as e:
                print(f"[{index+1}/{total}] ⚠️ Error loading {url}: {e}")
                if "Timeout" in str(e):
                    result["timeout"] = True
                if ENABLE_SCREENSHOT:
                    await page.screenshot(path=f"{SCREENSHOT_DIR}/{index+1}.png")
            finally:
                await browser.close()
    except Exception as e:
        print(f"[{index+1}/{total}] ❌ Critical error for {url}: {e}")
        result["timeout"] = True

    result["duration_sec"] = round(time.time() - start_time, 2)
    print(f"[{index+1}/{total}] ✅ Done in {result['duration_sec']}s\n")
    return result

async def extract_with_limit(url, index, total):
    async with sem:
        return await extract_head_elements(url, index, total)

async def process_urls_batch(urls, start_index):
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    total = len(urls)

    async def extract_with_limit(url, index):
        async with sem:
            return await extract_head_elements(url, index, total)

    tasks = [
        extract_with_limit(
            "http://" + url if not url.startswith("http") else url,
            start_index + idx
        )
        for idx, url in enumerate(urls)
    ]
    return await asyncio.gather(*tasks)


# ---------------- 실행부 ----------------
df = pd.read_csv(EXEC_FILE,low_memory=False)
urls = df["original_url"].dropna().tolist()
total_urls = len(urls)

all_result_paths = []

for batch_idx in range(0, total_urls, BATCH_SIZE):
    batch_urls = urls[batch_idx:batch_idx + BATCH_SIZE]
    print(f"\n🚀 Batch {batch_idx // BATCH_SIZE + 1}: Processing {len(batch_urls)} URLs...")

    results = asyncio.run(process_urls_batch(batch_urls, batch_idx))
    results_df = pd.DataFrame(results)

    result_file = f"{RESULT_DIR}/batch_{batch_idx // BATCH_SIZE + 1}.csv"
    results_df.to_csv(result_file, index=False, na_rep=NONE_DATA)
    all_result_paths.append(result_file)

# 結果統合
print("\n🧩 Merging all batch results...")
all_dfs = [pd.read_csv(path) for path in all_result_paths]
final_df = pd.concat(all_dfs, ignore_index=True)
final_df.to_csv("data/results/head_extraction_results.csv", index=False, na_rep=NONE_DATA)

print("🎉 全バッチ処理完了！結果は 'data/results/head_extraction_results.csv' に保存されました。")
