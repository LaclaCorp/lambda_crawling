#############################################################################################
# @name: bt_toto_result
# @Lambda function name: BETMAN_TOTO_RESULTS
# @description
# - 베트맨 토토 적중 결과 페이지 크롤링
# 예)
# 축구: https://www.betman.co.kr/main/mainPage/gamebuy/winrstDetl.do?gmId=G011&gmTs=250053&sbx_gmCase=&sbx_gmType=&ica_fromDt=2025.05.28&ica_endDt=2025.08.28&rdo=month3&curPage=3&perPage=10
# 야구: https://www.betman.co.kr/main/mainPage/gamebuy/winrstDetl.do?gmId=G024&gmTs=250062&sbx_gmCase=&sbx_gmType=&ica_fromDt=2025.05.28&ica_endDt=2025.08.28&rdo=month3&curPage=3&perPage=10
# 농구: https://www.betman.co.kr/main/mainPage/gamebuy/winrstDetl.do?gmId=G027&gmTs=250024&sbx_gmCase=TBK&sbx_gmType=G003,G004,G005,G015,G022,G025,G027,G041,G042,G055,G056&ica_fromDt=2025.01.01&ica_endDt=2025.03.31&rdo=&curPage=1&perPage=10
#############################################################################################

import os
import json
import datetime
from datetime import datetime as dt
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import pymongo
import env



# 환경설정
os.environ['TZ'] = 'Asia/Seoul'
KST = datetime.timezone(datetime.timedelta(hours=9))
database = env.database_environment()
MONGO_HOST = database['MONGO_HOST']
MONGO_PORT = database['MONGO_PORT']
MONGO_USERNAME = database['MONGO_USERNAME']
MONGO_PASSWORD = database['MONGO_PASSWORD']



import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options


def _new_driver():
    chrome_options = Options()
    # chrome_options.add_argument("--headless")
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--single-process")
    chrome_options.add_argument("--remote-debugging-port=9222")
    chrome_options.add_argument("--window-size=1280,1696")

    chrome_options.binary_location = "/usr/bin/google-chrome"
    service = Service(executable_path="/usr/local/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=chrome_options)

    driver.implicitly_wait(10)

    # ✅ Lambda 환경에서는 세션 attach까지 약간 딜레이 필요
    import time
    time.sleep(1)

    return driver



# bt_toto result
def lambda_handler(*args, **kwargs):


    # 1. DB 연동
    client = pymongo.MongoClient(
        host=MONGO_HOST, port=MONGO_PORT,
        username=MONGO_USERNAME, password=MONGO_PASSWORD,
        authSource="admin"
    )
    db = client['sliker']



    all_results = []
    toto_col = db.bt_totos
    gmTs = None

    if 'old_gmId_arr' in kwargs:
        gmId_arr = kwargs['old_gmId_arr']
    else:
        gmId_arr = ["G011", "G024", "G027"]
        # gmId_arr = ["G011"] 임시 주석


    # 브라우저 (드라이버) 호출
    # Selenium이 띄운 웹 브라우저 객체 : browser
    browser = _new_driver()

    for gmId in gmId_arr:

        if 'old_gmTs' in kwargs:
            old_gmTs = int(kwargs['old_gmTs'])
            recent_toto = toto_col.find({"gmId": gmId, "gmTs": old_gmTs})
        else:
            recent_toto = toto_col.find({"gmId": gmId}).sort("created_at", -1).limit(3) # 최근 3회차를 조회한다.



        for toto_data in recent_toto:

            try:
                gmTs = toto_data['gmTs']
                now = dt.now(tz=KST).strftime('%Y-%m-%d %H:%M:%S')
                url = 'https://www.betman.co.kr/main/mainPage/gamebuy/winrstDetl.do?gmId=' + gmId + '&gmTs=' + str(gmTs)
                print("url :", url)

                try:
                    browser.get(url)
                except TimeoutException:
                    print('ERROR******************[QUERY TIMEOUT]')
                except WebDriverException:
                    browser.get(url)


                soup = BeautifulSoup(browser.page_source, "html.parser")

                gmTs = int(gmTs)
                year = '20' + str(gmTs)[0:2]

                result_list = soup
                result_toto = result_list.find('tbody', {'id': 'tb_detlWdlPayo'}).findAll('tr')
                result_arr = ["rank", "past_amount", "total_amount", "result", "each_amount", "past_count", "total_past_amount"]
                result_num = 0
                result_db = []
                result_arr_data = {}
                result_db_num = 0

                if not result_toto or len(result_toto[0].find_all("td")) < len(result_arr):
                    print(f"[SKIP] gmId={gmId}, gmTs={gmTs} 아직 발표 안됨 (결과 없음)")
                    continue


                for result_td in result_toto:

                    for result_one in result_td:

                        # 현재 인덱스와 태그 출력
                        # print(f"[DEBUG] gmId={gmId}, gmTs={gmTs}, idx={result_num}, tag={result_one}")
                        # print(f"[DEBUG] text={result_one.getText().strip()}")
                        result_data = {
                            result_arr[result_num]: result_one.getText()
                        }
                        result_arr_data.update(result_data)
                        result_num += 1


                    result_db.append(result_arr_data)

                    # 1등, 2등..
                    # {'rank': '1등', 'past_amount': '965,256,750 원', 'total_amount': '1,749,589,000 원', 'result': '-', 'each_amount': '-', 'past_count': '3', 'total_past_amount': '1,749,589,000 원'}
                    toto_col.update_one({"gmId": gmId, "gmTs": gmTs}, {"$addToSet": {"results": result_arr_data}}, upsert=True)

                    # 확인 정보 로그
                    all_results.append({
                        "gmId": gmId,
                        "gmTs": gmTs,
                        "results": result_db
                    })

                    result_num = 0
                    result_db_num += 1
                    result_arr_data = {}



                events_results = result_list.find('div', {'id': 'grd_detlScBsBkWdl_wrapper'}).find('tbody').findAll('tr')
                detail_num = 0
                detail_arr = ["home_result", "away_result", "result"]
                detail_arr_data = []
                detail_db = {}


                for events_result_tr in events_results:
                    events_result_td = events_result_tr.findAll('td')
                    i = 0
                    while i < 3:
                        detail_data = {
                            detail_arr[i]: events_result_td[i + 3].getText()
                        }
                        detail_db.update(detail_data)
                        toto_col.update_one(
                            {"gmId": gmId, "gmTs": gmTs, "details." + str(detail_num) + ".num": events_result_td[0].
                                getText()}, {"$set": {
                                "details." + str(detail_num) + ".results." + detail_arr[i]: events_result_td[i + 3].getText()}},
                            upsert=True)
                        i += 1
                    detail_db = {}
                    detail_num += 1

            except Exception as e:
                print(f"[ERROR] {gmId}-{gmTs}: {e}")
                continue


    browser.quit()


    data = all_results
    body = data
    return {
        "isBase64Encoded": False,
        "headers": {
            "X-Requested-With": '*',
            "Access-Control-Allow-Headers": 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,x-requested-with',
            "Access-Control-Allow-Origin": '*',
            "Access-Control-Allow-Methods": 'POST,GET,OPTIONS',
            "content-type": "application/json"
        },
        'statusCode': 200, 'body': json.dumps(body)}
