#############################################################################################
# @name: bt_toto_old (updated full version)
# @description
# - 베트맨 토토 축구-승무패, 농구-승5패, 야구-승1패 크롤링
# - 과거 데이터 크롤링
#############################################################################################

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import os
import datetime
from datetime import datetime as dt
import pymongo
import json
import bt_toto_result
import env

MAXIMUM_VIDEO_LOAD = 1
os.environ['TZ'] = 'Asia/Seoul'
KST = datetime.timezone(datetime.timedelta(hours=9))
database = env.database_environment()
MONGO_HOST = database['MONGO_HOST']
MONGO_PORT = database['MONGO_PORT']
MONGO_USERNAME = database['MONGO_USERNAME']
MONGO_PASSWORD = database['MONGO_PASSWORD']


# ✅ 새로운 드라이버 생성 함수
def _new_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--single-process")
    chrome_options.add_argument("--remote-debugging-port=9222")
    chrome_options.add_argument("--window-size=1280,1696")
    chrome_options.add_argument("--lang=ko")

    chrome_options.binary_location = "/usr/bin/google-chrome"
    service = Service(executable_path="/usr/local/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=chrome_options)

    driver.implicitly_wait(10)

    import time
    time.sleep(1)  # ✅ Lambda 환경에서 세션 안정화용

    return driver


def get_html(url):
    browser = _new_driver()
    try:
        browser.get(url)
    except TimeoutException:
        print('ERROR******************[QUERY TIMEOUT]')
    except WebDriverException:
        browser.get(url)

    soup = BeautifulSoup(browser.page_source, "html.parser")

    recent_url = ''
    gmId = ''
    gmTs = ''
    browser.find_element("xpath", '//*[@id="content"]/div/div[1]/div[2]/div/span').click()
    browser.find_element("xpath", '//*[@id="content"]/div/div[1]/div[2]/div/span').click()
    proto_select = browser.find_element("xpath", '//*[@id="selectBoxGameRnd"]')
    browser.execute_script("arguments[0].setAttribute('style', 'display: block;')", proto_select)
    sel_data = browser.find_element("id", 'selectBoxGameRnd').find_elements("tag name", 'option')

    gmId = sel_data[0].get_attribute('value')[0:4]
    gmTs = sel_data[0].get_attribute('value')[5:]
    recent_url = 'http://www.betman.co.kr/main/mainPage/gamebuy/closedGameSlip.do?gmId=' + gmId + '&gmTs=' + gmTs

    browser.quit()

    return {
        "recent_url": recent_url,
        "gmId": gmId,
        "gmTs": gmTs,
        "soup": soup
    }


# bt_toto
def lambda_handler(*args, **kwargs):
    client = pymongo.MongoClient(host=MONGO_HOST, port=MONGO_PORT, username=MONGO_USERNAME, password=MONGO_PASSWORD,
                                 authSource="admin")
    db = client['sliker']

    toto_col = db.bt_totos
    name_matches_col = db.name_matches
    events_col = db.statscore_events

    # gmId_arr = ["G011", "G024", "G027"]
    gmId_arr = ["G024"]

    gmTs = 250046
    while gmTs < 250064:
        print(gmTs)
        for gmId in gmId_arr:
            if gmId == "G011":
                sport_type = 'Soccer'
                sport_id = 5
            elif gmId == "G024":
                sport_type = 'Baseball'
                sport_id = 7
            else:
                sport_type = 'BasketBall'
                sport_id = 1

            now = dt.now(tz=KST).strftime('%Y-%m-%d %H:%M:%S')
            url = 'https://www.betman.co.kr/main/mainPage/gamebuy/closedGameSlip.do?gmId=' + gmId + '&gmTs=' + str(gmTs)

            browser = _new_driver()
            try:
                try:
                    browser.get(url)
                except TimeoutException:
                    print('ERROR******************[QUERY TIMEOUT]')
                except WebDriverException:
                    browser.get(url)

                soup = BeautifulSoup(browser.page_source, "html.parser")

                browser.find_element("xpath", '//*[@id="content"]/div/div[1]/div[2]/div/span').click()
                browser.find_element("xpath", '//*[@id="content"]/div/div[1]/div[2]/div/span').click()
                list_s = soup

                gmTs = int(gmTs)
                year = '20' + str(gmTs)[0:2]
                toto = list_s.find('tbody', {'id': 'grid_victory_tbody'}).findAll('tr')

                toto_div = list_s.find('div', {"class": "gametopArea"}).findAll('ul')
                num = 0
                db_data = {}
                for_status = True
                arr = ["total_amount", "start_date", "end_date", "estimated_amount", "total_vote"]

                for toto_ul in toto_div:
                    toto_li = toto_ul.findAll('li')
                    for li_data in toto_li:
                        li_arr = li_data.getText().split('\n')
                        if num == 0:
                            if li_arr[2] == '원':
                                for_status = False
                                break
                            if not for_status:
                                break
                        if len(li_arr) > 1:
                            if num == 1:
                                start_year = li_arr[2][0:2]
                                start_month = li_arr[2][3:5]
                                start_day = li_arr[2][6:8]
                                start_hour = li_arr[2][12:14]
                                start_min = li_arr[2][15:17]

                                end_year = li_arr[2][20:22]
                                end_month = li_arr[2][23:25]
                                end_day = li_arr[2][26:28]
                                end_hour = li_arr[2][32:34]
                                end_min = li_arr[2][35:37]

                                start_date = datetime.datetime(int('20' + start_year), int(start_month), int(start_day),
                                                               int(start_hour),
                                                               int(start_min)).strftime("%Y-%m-%d %H:%M")

                                end_date = datetime.datetime(int('20' + end_year), int(end_month), int(end_day),
                                                             int(end_hour),
                                                             int(end_min)).strftime("%Y-%m-%d %H:%M")
                                arr_data = {
                                    arr[num]: start_date
                                }
                                db_data.update(arr_data)
                                arr_data = {
                                    arr[num + 1]: end_date
                                }
                                num += 1
                            else:
                                arr_data = {
                                    arr[num]: li_arr[2]
                                }
                            db_data.update(arr_data)
                        elif num == 6:
                            total_vote = li_arr[0].split('/')
                            arr_data = {
                                arr[4]: total_vote[0].replace("- 전체 투표수: ", "").strip()
                            }
                            db_data.update(arr_data)
                        num += 1

                toto_game = {
                    "gmId": gmId,
                    "gmTs": gmTs,
                    "sport_type": sport_type,
                    "sport_id": sport_id
                }
                if not for_status:
                    continue
                if db_data["total_amount"] == '원':
                    break
                db_data.update(toto_game)
                toto_exist = toto_col.find_one({'gmId': gmId, "gmTs": gmTs})
                if toto_exist:
                    toto_col_status = "exist"
                    toto_date_db = {
                        "updated_at": dt.now()
                    }
                else:
                    toto_col_status = None
                    toto_date_db = {
                        "created_at": dt.now(),
                        "updated_at": dt.now()
                    }
                db_data.update(toto_date_db)
                toto_col.update_one({'gmId': gmId, 'gmTs': gmTs}, {"$set": db_data}, upsert=True)
                toto_col_data = toto_col.find_one({'gmId': gmId, 'gmTs': gmTs})

                # bt_toto details document
                toto_arr = ["num", "start_date", "place", "home_name", "away_name", "win", "win_vote", "draw", "draw_vote",
                            "lose", "lose_vote"]
                key_num = 0
                toto_db = {}
                toto_detail_list = []
                db_num = 0
                for toto_tr in toto:
                    toto_td = toto_tr.findAll('td')
                    key_num = 0
                    for td_one in toto_td:
                        td_one_text = td_one.getText()
                        if key_num == 0:
                            td_one_text = td_one_text.replace("긴급 공지닫기", "")
                        if key_num == 1:
                            td_one_arr = td_one_text.split("\n")
                            toto_data = {
                                toto_arr[key_num]: td_one_arr[0]
                            }
                            if toto_col_status is not None:
                                toto_col.update_one(
                                    {"gmId": gmId, "gmTs": gmTs,
                                     "details." + str(db_num) + ".num": {
                                         "$in": [toto_td[0].getText().replace("긴급 공지닫기", "")]}},
                                    {"$set": {"details." + str(db_num) + "." + toto_arr[key_num]: td_one_arr[0]}},
                                    upsert=True)
                            key_num += 1
                            td_one_text = td_one_arr[1].replace("경기장", "")
                            toto_db.update(toto_data)
                        elif key_num == 3:
                            td_one_arr = td_one_text.split("vs ")
                            toto_data = {
                                toto_arr[key_num]: td_one_arr[0]
                            }
                            if toto_col_status is not None:
                                toto_col.update_one(
                                    {"gmId": gmId, "gmTs": gmTs,
                                     "details." + str(db_num) + ".num": {
                                         "$in": [toto_td[0].getText().replace("긴급 공지닫기", "")]}},
                                    {"$set": {"details." + str(db_num) + "." + toto_arr[key_num]: td_one_arr[0]}},
                                    upsert=True)
                            td_one_text = td_one_arr[1]
                            key_num += 1
                            toto_db.update(toto_data)
                        elif (key_num > 3) and (key_num < 10):
                            td_one_arr = td_one_text.split("(")
                            toto_data = {
                                toto_arr[key_num]: td_one_arr[0].replace("투표율", "")
                            }
                            if toto_col_status is not None:
                                toto_col.update_one(
                                    {"gmId": gmId, "gmTs": gmTs,
                                     "details." + str(db_num) + ".num": {
                                         "$in": [toto_td[0].getText().replace("긴급 공지닫기", "")]}},
                                    {"$set": {
                                        "details." + str(db_num) + "." + toto_arr[key_num]: td_one_arr[0].replace("투표율",
                                                                                                                  "")}},
                                    upsert=True)
                            td_one_text = td_one_arr[1].replace(")", "")
                            key_num += 1
                            toto_db.update(toto_data)
                        elif key_num >= 10:
                            break
                        td_one_text = td_one_text.replace("긴급 공지닫기", "")
                        toto_data = {
                            toto_arr[key_num]: td_one_text
                        }
                        if toto_col_status is not None:
                            toto_col.update_one(
                                {"gmId": gmId, "gmTs": gmTs},
                                {"$set": {"details." + str(db_num) + "." + toto_arr[key_num]: td_one_text}},
                                upsert=True)
                        key_num += 1
                        toto_db.update(toto_data)
                    if toto_col_status is None:
                        toto_col.update_one({"gmId": gmId, "gmTs": gmTs},
                                            {"$addToSet": {"details": toto_db}},
                                            upsert=True)
                    home = name_matches_col.find_one({"bt_name": toto_db['home_name'], "sport_id": sport_id})
                    away = name_matches_col.find_one({"bt_name": toto_db['away_name'], "sport_id": sport_id})

                    if home is None:
                        home = name_matches_col.find_one({"old_bt_name": toto_db['home_name'], "sport_id": sport_id})
                    if away is None:
                        away = name_matches_col.find_one({"old_bt_name": toto_db['away_name'], "sport_id": sport_id})

                    if home and away:
                        home_id = home['participant_id']
                        away_id = away['participant_id']
                        month = toto_db['start_date'][3:5]
                        day = toto_db['start_date'][6:8]
                        start_date_length = len(toto_db['start_date'])
                        hour = toto_db['start_date'][start_date_length - 5:start_date_length - 3]
                        minute = toto_db['start_date'][start_date_length - 2:start_date_length]
                        start_date = datetime.datetime(int(year), int(month), int(day), int(hour), int(minute)) \
                            .strftime("%Y-%m-%d %H:%M")
                        event_data = events_col.find_one({"event.start_date": str(start_date),
                                                          "event.participants.id": {"$all": [home_id, away_id]}})
                        if event_data:
                            event_db = {
                                "event_id": event_data['event']['id'],
                                "sport_id": event_data['sport_id'],
                                "home_id": home_id,
                                "away_id": away_id,
                            }
                            event_arr = ["event_id", "sport_id", "home_id", "away_id"]
                            event_db_data = [event_data['event']['id'], event_data['sport_id'], home_id, away_id]
                            toto_db.update(event_db)
                            toto_detail_list.append(toto_db)
                            event_arr_num = 0
                            for event_arr_data in event_arr:
                                toto_col.update_one({"gmId": gmId, "gmTs": gmTs,
                                                     "details." + str(db_num) + ".num": {
                                                         "$in": [toto_td[0].getText().replace("긴급 공지닫기", "")]}},
                                                    {"$set": {
                                                        "details." + str(db_num) + "." + event_arr_data: event_db_data[
                                                            event_arr_num]}},
                                                    upsert=True)
                                event_arr_num += 1
                            toto_db = {}
                    db_num += 1
            finally:
                try:
                    browser.quit()
                except:
                    pass
        bt_toto_result.lambda_handler(old_gmTs=gmTs, old_gmId_arr=gmId_arr)
        gmTs += 1
    data = ''
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
