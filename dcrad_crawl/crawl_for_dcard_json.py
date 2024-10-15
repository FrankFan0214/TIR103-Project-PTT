# 可以把dcard的資訊存成json
# 但因為是用selenium，所以效率不是很好，爬得有點慢
# 可以爬取30篇
#------------------------------------------------------
from selenium.webdriver.common.by import By
from time import sleep
import undetected_chromedriver as uc
import os
from selenium.webdriver.common.keys import Keys
import re
import json

# 指定保存 Excel 檔案的路徑
save_path = "./dcard_article.json"

# 如果已經有檔案存在，讀取已爬取的資料
if os.path.exists(save_path):
    with open(save_path, 'r', encoding='utf-8') as f:
        # 將json檔案存成list
        existing_data = json.load(f)
        # 將標題取出來
        existing_titles = set(post['標題'] for post in existing_data)
else:
    existing_titles = set()  # 如果檔案不存在，表示沒有已經爬取的資料
    existing_data = []

# 使用 undetected_chromedriver 初始化 Chrome 瀏覽器 (用來爬標題和連結)
driver = uc.Chrome()
# 前往dcard
dcard_url = "https://www.dcard.tw/f" 
driver.get(dcard_url)

# 等待頁面載入
sleep(10)

# 儲存所有文章的標題、連結
data = []

# 一次爬幾個文章
num_articles = 30

for i in range(1, num_articles+1):
    # 抓取連結
    element_by_data_key = driver.find_element(By.XPATH, f"//div[@data-key='{i}']")
    url = element_by_data_key.find_element(By.TAG_NAME, "a").get_attribute("href")
    # 抓取文章ID
    article_ID = url.split('/')[-1]
    # 抓取標題
    element_by_a_tag = element_by_data_key.find_element(By.TAG_NAME, "a")
    title = element_by_a_tag.find_element(By.TAG_NAME, "span")
    # 使用第二個瀏覽器(用來爬文章內容)
    driver1 = uc.Chrome()
    driver1.get('https://www.google.com')
    # 定位google搜尋的位置
    search = driver1.find_element(By.NAME, "q")
    search.send_keys(f"{url}")
    search.send_keys(Keys.ENTER)
    sleep(10)
    # 如果搜尋不到文章，就會跳過
    try:
        # 進入dcard文章
        driver1.find_element(By.XPATH, f"//*[@id='rso']/div[1]/div/div/div/div[1]/div/div/span/a[@href='{url}']").click()
        sleep(10)
    except Exception:
        driver1.quit()
        driver.execute_script("arguments[0].scrollIntoView({block:'start'});", element_by_data_key)
        continue
    # 抓取看版類型
    type = driver1.find_element(By.CLASS_NAME, f"tcjsomj").text   
    # 抓取作者
    author = driver1.find_element(By.CLASS_NAME, f"avvspio").text
    # 抓取發布時間(抓的時間是格林威治標準時間，所以還要再+8才是台灣時間)
    time = driver1.find_element(By.TAG_NAME, "time").get_attribute('title')
    # 抓取emoji數
    try:
        emoji_num = driver1.find_element(By.CLASS_NAME, f"s1r6dl9").text
    except Exception:
        emoji_num = 0
    # 抓取留言數
    try:
        message_num = driver1.find_element(By.CLASS_NAME, f"c1gbs76y").text
    except Exception:
        message_num = 0
    # 抓取hash tag
    try:
        hash_tag = []
        element_by_hash_tag = driver1.find_element(By.XPATH, '//*[@id="__next"]/div[2]/div[2]/div/div/div/div/article/div[3]/div').text
        for word in element_by_hash_tag.split('\n'):
            hash_tag.append(word)
    except Exception:
        hash_tag = []
    # 抓取文章內容(修改)
    article_content = ''
    element_by_class = driver1.find_element(By.XPATH, '//*[@id="__next"]/div[2]/div[2]/div/div/div/div/article/div[2]/div/div')
    element_by_span = element_by_class.find_elements(By.TAG_NAME, "span")
    # 將文章內容存成字串
    for span in element_by_span:
        content = re.findall(r'.{1}',span.text)
        for word in content:
            article_content += word
    # 進入emoji小頁面
    try:
        test = driver1.find_element(By.CLASS_NAME, 'r1skb6m4')
        driver1.execute_script("arguments[0].scrollIntoView({block:'center'});", test)
        driver1.find_element(By.CLASS_NAME, 'r1skb6m4').click()
        # 各個emoji數
        emojis = []
        type_emoji = {}
        element_by_emojis = driver1.find_elements(By.CLASS_NAME, 'irn7u4a')
        for each in element_by_emojis:
            key = each.text.split('\n')[0]
            value  = each.text.split('\n')[1]
            type_emoji[key] = value
        emojis.append(type_emoji)
    except Exception:
        emojis = []
    # 離開文章
    sleep(10)
    driver1.quit()

    if title.text not in existing_titles:
        data.append({"文章ID": article_ID, "作者": author, "標題": title.text, "連結": url, "發布時間": time, "內容": article_content, "總emoji數": emoji_num, " emoji類型": emojis, "留言數": message_num, "hash_tag": hash_tag, "看版": type})
        existing_titles.add(title.text)
    # 頁面向下滾動
    driver.execute_script("arguments[0].scrollIntoView({block:'start'});", element_by_data_key)
    sleep(10)
driver.quit()
# 將新爬取的資料合併至已有資料中
if data:
    existing_data.extend(data)
    # 將資料寫入 JSON 檔案
    with open(save_path, 'w', encoding='utf-8') as f:
        # indent: 每行開頭要空幾格，ensuree_ascii = False: 不會用ascii編碼(可以顯示中文)
        json.dump(existing_data, f, ensure_ascii=False, indent=4)

    print(f"成功將新資料保存至 {save_path}")
else:
    print("沒有新資料可以追加。")