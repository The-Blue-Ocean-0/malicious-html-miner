import pandas as pd
import asyncio
import time
import json
import os
import re
from playwright.async_api import async_playwright

# ------------------- 설정 -------------------
RESULT_FILE = 'data/results/head_extraction_results.csv' # 결과 파일 경로
EXEC_FILE = 'data/test/test_url_data.csv' # 실행할 파일 경로
SCREENSHOT_DIR = 'data/screenshots' # 스크린샷 저장 경로
ENABLE_SCREENSHOT = False     # 스크린샷 촬영 여부
LIMIT = 20                    # 처리할 첫 번째 몇 개의 URL (None이면 모든 URL 처리)
MERGE_ORIGINAL = False         # 원본 데이터와 결과를 결합할지 여부
NONE_DATA = 'null'            # 데이터가 없을 때 표시할 문자
# --------------------------------------------

os.makedirs("screenshots", exist_ok=True)

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
                    attributes = await tag.evaluate(
                        "(el) => Object.fromEntries([...el.attributes].map(attr => [attr.name, attr.value]))")
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
                        await page.screenshot(path=f"SCREENSHOT_DIR/{index+1}.png")

            except Exception as e:
                print(f"[{index+1}/{total}] ⚠️ Error loading {url}: {e}")
                if "Timeout" in str(e):
                    result["timeout"] = True
                if ENABLE_SCREENSHOT:
                    await page.screenshot(path=f"SCREENSHOT_DIR/{index+1}.png")
            finally:
                await browser.close()
    except Exception as e:
        print(f"[{index+1}/{total}] ❌ Critical error for {url}: {e}")
        result["timeout"] = True

    result["duration_sec"] = round(time.time() - start_time, 2)
    print(f"[{index+1}/{total}] ✅ Done in {result['duration_sec']}s\n")
    return result

async def process_urls(urls):
    total = len(urls)
    tasks = [extract_head_elements(
                "http://" + url if not url.startswith("http") else url,
                idx,
                total
             ) for idx, url in enumerate(urls)]
    return await asyncio.gather(*tasks)

# ---------------- 실행부 ----------------
df = pd.read_csv(EXEC_FILE)
urls = df["original_url"].dropna().tolist()
target_count = LIMIT if LIMIT is not None else len(urls)

# URL 처리 실행
results = asyncio.run(process_urls(urls[:target_count]))
results_df = pd.DataFrame(results)

# 병합 여부에 따른 처리
if MERGE_ORIGINAL:
    # 중복 컬럼 제거
    columns_to_drop = results_df.columns.intersection(df.columns)
    results_df = results_df.drop(columns=columns_to_drop)

    # 원본 데이터와 병합
    merged_df = df.iloc[:target_count].reset_index(drop=True).join(results_df)
    merged_df.to_csv(RESULT_FILE, index=False, na_rep=NONE_DATA)
else:
    results_df.to_csv(RESULT_FILE, index=False, na_rep=NONE_DATA)

print(f"🎉 완료！결과는 '{RESULT_FILE}'에 저장되었습니다")

