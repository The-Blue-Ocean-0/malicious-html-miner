import asyncio
from playwright.async_api import async_playwright

async def extract_head_elements(url):
    elements = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            await page.goto(url, timeout=10000, wait_until='domcontentloaded')

            # headタグ内のすべての直下要素を取得（meta, title, link, scriptなど）
            head_children = await page.query_selector_all("head > *")
            for tag in head_children:
                tag_name = await tag.evaluate("(el) => el.tagName.toLowerCase()")
                attributes = await tag.evaluate("(el) => Object.fromEntries([...el.attributes].map(attr => [attr.name, attr.value]))")
                text = await tag.inner_text()

                elements.append({
                    "tag": tag_name,
                    "attributes": attributes,
                    "text": text.strip() if text else ""
                })

        except Exception as e:
            print(f"❌ Error loading {url}: {e}")
        finally:
            await browser.close()

    return elements

# テスト用実行
test_url = "https://mp3raid.com"
elements = asyncio.run(extract_head_elements(test_url))

# 表示整形
for i, el in enumerate(elements):
    print(f"[{i}] <{el['tag']}>")
    for attr, val in el['attributes'].items():
        print(f"    @{attr} = {val}")
    if el['text']:
        print(f"    text: {el['text']}")
