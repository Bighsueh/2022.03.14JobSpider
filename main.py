import concurrent
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup as bts
import requests as rq
import pandas as pd

thread = 32  # thread數量
# 目標網站 1111人力銀行 關鍵字：大數據
base_url = "https://www.1111.com.tw/search/job?ks=%E5%A4%A7%E6%95%B8%E6%93%9A"

# 依照總資料筆數自動設定要爬的頁數
response = rq.get(base_url)
html_parse = bts(response.text, 'html.parser')
job_count = html_parse.find('div', {'class': ['nav_item', 'job_count']})['data-count']
page = int(float(job_count.replace(',','')) / 10)
print(f"發現{job_count}筆資料")

# 目標url
target_urls = [f"{base_url}&page={num_page}" for num_page in range(1, page + 1)]

# 先建立dataframe
columns = {
    'job_title': [],
    'place': [],
    'company': [],
    'salary': [],
    'job_info': [],
}
df = pd.DataFrame(columns)


def get_job_info(target_url):
    global df
    response = rq.get(target_url)
    html_parse = bts(response.text, 'html.parser')
    # 如果沒有找到新資訊就break
    job_containers = html_parse.find_all("div", {'class': ['item__job', 'job_item']})
    if job_containers == []:
        return
    # 讀出工作資訊
    for job_container in job_containers:
        job_title = job_container.find('h5', {'class': ['card-title', 'title_6']}).text
        place = job_container.find('a', {'class': 'job_item_detail_location'}).text
        company = job_container.find('h6', {'class': 'job_item_company'}).text
        salary = job_container.find('div', {'class': 'job_item_detail_salary'}).text
        job_info = job_container.find('p', {'class', 'job_item_description'}).text

        # 將資料塞進dataframe
        row = {
            'job_title': job_title,
            'place': place,
            'company': company,
            'salary': salary,
            'job_info': job_info
        }
        df = df.append(row, ignore_index=True)
    print(f"已處理{df.shape[0]}筆資料")


print('開始搜尋資料')
start_time = time.time()

# 用4個執行序去爬資料
with ThreadPoolExecutor(max_workers=thread) as executor:
    executor.map(get_job_info, target_urls)

end_time = time.time()
print('搜尋結束')
print(f'{end_time - start_time}爬取{page}頁，共{df.shape[0]}筆資料')
df.to_csv('data.csv', index=False, encoding='utf_8_sig')
