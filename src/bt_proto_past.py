#############################################################################################
# @name: bt_proto_old (리팩토링 버전 - Lambda 컨테이너 호환)
# @description
# - 회차를 지정해서 수동으로 돌리기 위함
# - AWS Lambda 컨테이너 및 로컬 환경 모두 지원
#############################################################################################

import os
import json
import datetime
from datetime import datetime as dt
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
import pymongo
import env
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(message)s')

# 환경 변수 및 DB 설정
os.environ['TZ'] = 'Asia/Seoul'
KST = datetime.timezone(datetime.timedelta(hours=9))
database = env.database_environment()
MONGO_HOST = database['MONGO_HOST']
MONGO_PORT = database['MONGO_PORT']
MONGO_USERNAME = database['MONGO_USERNAME']
MONGO_PASSWORD = database['MONGO_PASSWORD']

## DB 연동 처리
def connect_mongo():
    """MongoDB 연결"""
    try:
        db_name = database.get('MONGO_DB', 'sliker')  # 환경변수에서 DB명 가져오기
        client = pymongo.MongoClient(
            host=MONGO_HOST,
            port=MONGO_PORT,
            username=MONGO_USERNAME,
            password=MONGO_PASSWORD,
            authSource="admin",
            serverSelectionTimeoutMS=5000
        )
        return client[db_name]
    except Exception as e:
        logging.error(f"MongoDB 연결 실패: {e}")
        return None








# ------------------------------
# Selenium 드라이버 초기화 (Lambda 컨테이너 & 로컬 자동 감지)
# ------------------------------
def _new_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1280x1696")

    driver_path = "/var/task/bin/chromedriver"
    chrome_path = "/var/task/bin/chrome"

    os.chmod(driver_path, stat.S_IRWXU)
    os.chmod(chrome_path, stat.S_IRWXU)

    chrome_options.binary_location = chrome_path
    service = Service(executable_path=driver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver





def get_html(url):
    """URL 접속 후 BeautifulSoup 객체 반환"""
    browser = _new_driver()

    try:
        browser.get(url)
    except TimeoutException:
        print('ERROR******************[QUERY TIMEOUT]')
    except WebDriverException:
        browser.get(url)


    soup = BeautifulSoup(browser.page_source, "html.parser")
    try:
        browser.find_element(By.XPATH, '//*[@id="content"]/div/div[1]/div[2]/div/span').click()
    except NoSuchElementException:
        logging.warning("첫 클릭 요소를 찾지 못했습니다.")

    proto_select = browser.find_element(By.ID, 'selectBoxGameRnd')
    browser.execute_script("arguments[0].setAttribute('style', 'display: block;')", proto_select)
    sel_data = proto_select.find_elements(By.TAG_NAME, 'option')
    gmId = sel_data[0].get_attribute('value')[0:4]
    gmTs = sel_data[0].get_attribute('value')[5:]

    recent_url = f"http://www.betman.co.kr/main/mainPage/gamebuy/closedGameSlip.do?gmId={gmId}&gmTs={gmTs}"
    browser.quit()

    return {
        "recent_url": recent_url,
        "gmId": gmId,
        "gmTs": gmTs,
        "soup": soup
    }


def lambda_handler(*args, **kwargs):
    gmId = 'G101'
    gmTs = 220037
    end_gmTs = 220038  # 마지막 회차

    db = connect_mongo()
    if db is None:
        return


    while gmTs < end_gmTs:
        url = f"https://www.betman.co.kr/main/mainPage/gamebuy/closedGameSlip.do?gmId={gmId}&gmTs={gmTs}&gameDivCd=C"
        logging.info(f"[크롤링 시작] {gmId} - {gmTs}")

        browser = _new_driver()
        try:
            browser.get(url)
        except (TimeoutException, WebDriverException) as e:
            logging.error(f"[QUERY ERROR] {e}")
            browser.quit()
            gmTs += 1
            continue

        soup = BeautifulSoup(browser.page_source, "html.parser")
        try:
            browser.find_element(By.XPATH, '//*[@id="content"]/div/div[1]/div[2]/div/span').click()
        except NoSuchElementException:
            logging.warning("경기 리스트 여는 클릭 요소를 찾지 못했습니다.")

        tbody = soup.find('tbody', {'id': 'tbd_gmBuySlipList'})
        toto_rows = tbody.find_all('tr') if tbody else []
        toto = [row for row in toto_rows if row.get('id') != 'protoNoDataArea']

        proto_col = db.bt_protos
        lambda_protos_col = db.lambda_protos

        total_count = 1
        # 여기서 td_data 파싱 로직 이어붙이기
        # ...

        lambda_protos_col.update_one({"gmId": gmId, "gmTs": gmTs},
                                     {"$set": {"total_count": total_count, "updated_at": dt.now()}},
                                     upsert=True)

        browser.quit()
        gmTs += 1

    return {
        "isBase64Encoded": False,
        "headers": {
            "Access-Control-Allow-Origin": '*',
            "Access-Control-Allow-Methods": 'POST,GET,OPTIONS',
            "content-type": "application/json"
        },
        'statusCode': 200,
        'body': json.dumps({"message": "완료"})
    }
