#############################################################################################
# @name: bt_proto
# @Lambda function name: BETMAN-PROTOS
# @description
# - 베트맨 프로토 크롤링 (최신 회차)
# - Lambda 컨테이너용: Selenium 4 스타일 / Chromium & chromedriver는 베이스 이미지에서 제공
#############################################################################################

import os, stat
import json
import time
import datetime
from datetime import datetime as dt

import pymongo
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging
import env



MAXIMUM_VIDEO_LOAD = 1
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
        db_name = database.get('MONGO_DB', 'sliker')
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



def debug_db_data(tag: str, data: dict):
    print(f"\n📌 [{tag}] 현재까지의 db_data 상태:")
    for k, v in data.items():
        print(f"  - {k}: {v}")


# ------------------------------
# Selenium 드라이버 초기화 (Lambda 컨테이너용)
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


# 최신
def get_html(url):
    browser = _new_driver()
    try:
        try:
            browser.get(url)
        except TimeoutException:
            print('ERROR******************[QUERY TIMEOUT]')
        except WebDriverException:
            browser.get(url)

        soup = BeautifulSoup(browser.page_source, "html.parser")

        # 정상 원본 : 조회 버튼을 두 번 클릭한다.
        # 한번 클릭하면 셀렉트박스 내용이 로딩되어 들어온다. 그래서 두번 클릭
        # Selenium 4: find_element(By.XPATH, ...)
        browser.find_element(By.XPATH, '//*[@id="content"]/div/div[1]/div[2]/div/span').click()
        browser.find_element(By.XPATH, '//*[@id="content"]/div/div[1]/div[2]/div/span').click()


        proto_select = browser.find_element(By.ID, 'selectBoxGameRnd')
        # select 표시를 위해 style 조작
        browser.execute_script("arguments[0].setAttribute('style', 'display: block;')", proto_select)
        sel_data = proto_select.find_elements(By.TAG_NAME, 'option')

        # 최신 최상위 회차의 파라미터를 얻는다.
        gmId = sel_data[0].get_attribute('value')[0:4]  # G101
        gmTs = sel_data[0].get_attribute('value')[5:]   # 250087
        print("get_html의  gmTs :"+gmTs)

        recent_url = f'http://www.betman.co.kr/main/mainPage/gamebuy/gameSlip.do?gmId={gmId}&gmTs={gmTs}'
        print("최신 회차: "+recent_url)
        time.sleep(3)

        return {
            "recent_url": recent_url,
            "gmId": gmId,
            "gmTs": gmTs,
            "soup": soup,
        }
    finally:
        try:
            browser.quit()
        except Exception:
            pass


def lambda_handler(*args, **kwargs):

    gmId = 'G101'
    gmTs = 250093
    url = f'https://www.betman.co.kr/main/mainPage/gamebuy/gameSlip.do?gmId={gmId}&gmTs={gmTs}&gameDivCd=C'

    logging.basicConfig(level=logging.INFO)
    logging.info(f"최초 url: {url}")

    # 최신회차 정보 먼저 로드
    recent = get_html(url)
    url = recent['recent_url']  # 최신 회차 URL
    gmId = recent['gmId']       # 최신 회차 타입 G101
    gmTs = recent['gmTs']       # 최신 번호

    browser = _new_driver()

    try:
        try:
            browser.get(url)
        except TimeoutException:
            print('ERROR******************[QUERY TIMEOUT]')
        except WebDriverException:
            browser.get(url)

        # 첫 클릭 (여기서 최신 페이지로 이동한다)
        browser.find_element(By.XPATH, '//*[@id="content"]/div/div[1]/div[2]/div/span').click()

        soup = BeautifulSoup(browser.page_source, "html.parser")
        time.sleep(2)
        tbody = soup.find('tbody', {'id': 'tbd_gmBuySlipList'})
        toto_rows = tbody.find_all('tr') if tbody else []

        # "noData" 행 제거
        toto = [row for row in toto_rows if row.get('id') != 'protoNoDataArea']

        if not toto:
            print("[INFO] 발매중인 게임이 없습니다. 수집 중단.")
            return {
                "statusCode": 200,
                "body": json.dumps({"message": "no active games"}, ensure_ascii=False)
            }


        ## DB 연동 처리
        db = connect_mongo()
        if db is None:
            logging.error("MongoDB 연결 실패로 작업 중단")
            return {
                "statusCode": 500,
                "body": json.dumps({"error": "DB connection failed"}, ensure_ascii=False)
            }

        # Collections
        proto_col = db.bt_protos
        name_matches_col = db.name_matches
        events_col = db.statscore_events
        lambda_protos_col = db.lambda_proto

        change_num = 0
        db_data = {}
        toto_data = ""
        game_type = ""
        sport_id = ""

        year = '20' + str(gmTs)[0:2]
        gmTs = int(gmTs)
        total_count = 1



        for toto_td in toto:
            no = 0
            db_data = {"gmId": gmId, "gmTs": gmTs}

            print("+" * 40, no, "+" * 40)
            print(BeautifulSoup(str(toto_td), "html.parser").prettify())
            print("+" * 80)

            td_list = toto_td.find_all('td')

            for num, td_data in enumerate(td_list):
                if num == 5:
                    print("=" * 80)
                    print(BeautifulSoup(str(td_data), "html.parser").prettify())
                    print("=" * 80)

                if num == 0:
                    change_num = td_data.getText().replace("긴급 공지닫기", "")
                    db_data.update({"num": int(change_num)})
                    debug_db_data("num 처리 후", db_data)

                elif num == 1:
                    end_date_text = td_data.getText()
                    end_date = end_date_text
                    if end_date_text == "미정":
                        break
                    elif end_date_text == "결과발표":
                        end_date = "결과발표"
                    elif " 마감" in end_date_text:
                        end_date_text = end_date_text.replace(" 마감", "")
                        month = end_date_text[0:2]
                        day = end_date_text[3:5]
                        end_date_length = len(end_date_text)
                        hour = end_date_text[end_date_length - 5:end_date_length - 3]
                        minute = end_date_text[end_date_length - 2:end_date_length]
                        end_date = datetime.datetime(int(year), int(month), int(day), int(hour), int(minute)).strftime("%Y-%m-%d %H:%M")

                    db_data.update({"end_date": end_date})
                    debug_db_data("1 처리 후", db_data)

                elif num == 2:
                    sport_name = td_data.getText()[0:2]
                    if sport_name == '축구':
                        sport_id = 5
                    elif sport_name == '농구':
                        sport_id = 1
                    elif sport_name == '배구':
                        sport_id = 2
                    else:
                        sport_id = 7

                    db_data.update({
                        "sport_name": sport_name,
                        "league_name": td_data.getText()[2:]
                    })
                    debug_db_data("2 처리 후", db_data)

                elif num == 3:
                    game_type_data = td_data.find('span', {'class': 'badge'})
                    game_type = game_type_data.getText()
                    debug_db_data("3 처리 후", db_data)

                elif num == 4:
                    home_span = td_data.find('div', {'class': 'cell tar'}).find_all('span')
                    for home_data in home_span:
                        if 'H ' in home_data.getText():
                            features = home_data.getText().replace("사전조건 변경", "").split()
                            if game_type == "핸디캡":
                                db_data.update({
                                    "game_type_id": 2,
                                    "game_type": game_type,
                                    "feature": features[1]
                                })
                        elif 'U/O' in home_data.getText():
                            features = home_data.getText().replace("사전조건 변경", "").split()
                            if game_type == "언더오버":
                                db_data.update({
                                    "game_type_id": 3,
                                    "game_type": game_type,
                                    "feature": features[1]
                                })
                        else:
                            feature = "-"
                            if game_type == "일반":
                                db_data.update({
                                    "game_type_id": 1,
                                    "game_type": game_type,
                                    "feature": feature
                                })

                    away_span = td_data.find('div', {'class': 'cell tal'}).find_all('span')
                    home_name = home_span[0].getText()
                    away_name = away_span[len(away_span) - 1].getText()
                    db_data.update({
                        "home_name": home_name,
                        "away_name": away_name
                    })
                    debug_db_data("4 처리 후", db_data)

                elif num == 5:
                    odds_data = td_data.find_all('button')
                    odds_span = td_data.find_all('span', {'class': 'db'})
                    span_num = 0
                    for odd in odds_data:
                        toto_data = {}
                        odd_text = odd.getText()
                        odds_index = odds_span[span_num].getText().find("배")

                        if odds_index > 0:
                            odds_one = odds_span[span_num].getText()[0:odds_index]
                        else:
                            odds_one = odds_span[span_num].getText()

                        if odd_text[0] == "승":
                            key_name = "win"
                        elif odd_text[0] == "무":
                            key_name = "draw"
                        elif odd_text[0] == "패":
                            key_name = "lose"
                        elif odd_text[0] == "U":
                            key_name = "under"
                        elif odd_text[0] == "O":
                            key_name = "over"
                        elif ord(odd_text[0]) == 9312:
                            key_name = "draw1"
                        elif ord(odd_text[0]) == 9316:
                            key_name = "draw5"
                        else:
                            key_name = ""

                        if "발매차단" in odd_text:
                            odds_status = "block"
                        else:
                            odds_status = True

                        if key_name:
                            toto_data = {key_name: odds_one, key_name + "_status": odds_status}
                            db_data.update(toto_data)
                            debug_db_data("5 처리 후", db_data)
                        else:
                            print(f"[⚠️ 경고] 인식되지 않은 odd_text: {odd_text} → 스킵됨")

                        db_data.update(toto_data)
                        span_num += 1
                        debug_db_data("5 처리 후", db_data)

                elif num == 6:
                    date_text = td_data.getText()
                    month = date_text[0:2]
                    day = date_text[3:5]
                    start_date_length = len(date_text)
                    hour = date_text[start_date_length - 5:start_date_length - 3]
                    minute = date_text[start_date_length - 2:start_date_length]
                    start_date = datetime.datetime(int(year), int(month), int(day), int(hour), int(minute)).strftime("%Y-%m-%d %H:%M")
                    db_data.update({"start_date": start_date})
                    debug_db_data("6 처리 후", db_data)

                elif num == 7:
                    place_text = td_data.getText()
                    db_data.update({"place": place_text.replace("경기장", "")})
                    debug_db_data("7 처리 후", db_data)

                elif num == 8:
                    proto_col_data = proto_col.find_one({"gmId": gmId, "gmTs": gmTs, "num": int(change_num)})
                    if proto_col_data:
                        db_data.update({"updated_at": dt.now()})
                        if 'odds_changed_at' in proto_col_data:
                            pass
                        else:
                            db_data.update({"odds_changed_at": dt.now(tz=KST).strftime('%Y-%m-%d %H:%M:%S')})
                    else:
                        db_data.update({"created_at": dt.now(), "updated_at": dt.now()})

                    home = name_matches_col.find_one({"bt_name": db_data['home_name'], "sport_id": sport_id})
                    away = name_matches_col.find_one({"bt_name": db_data['away_name'], "sport_id": sport_id})

                    if home is None:
                        home = name_matches_col.find_one({"bt_name": db_data['home_name'], "sport_id": sport_id})
                    if away is None:
                        away = name_matches_col.find_one({"bt_name": db_data['away_name'], "sport_id": sport_id})

                    if home and away:
                        home_id = home['participant_id']
                        away_id = away['participant_id']
                        event_start_date = db_data['start_date']
                        event_data = events_col.find_one({"event.start_date": event_start_date, "event.participants.id": {"$all": [home_id, away_id]}}, sort=[('created_at', -1)])
                        if event_data:
                            event_db = {
                                "event_id": event_data['event']['id'],
                                "sport_id": event_data['sport_id'],
                                "home_id": home_id,
                                "away_id": away_id,
                            }
                            db_data.update(event_db)
                            events_col.update_one({"event.start_date": event_start_date, "event.participants.id": {"$all": [home_id, away_id]}}, {"$set": {"bt_proto_status": True}})
                        else:
                            event_db = {
                                "event_id": None,
                                "sport_id": home['sport_id'],
                                "home_id": home_id,
                                "away_id": away_id,
                            }
                            db_data.update(event_db)

                    proto_col.update_one({"gmId": gmId, "gmTs": gmTs, "num": int(change_num)}, {"$set": db_data}, upsert=True)

                    if '배당률 변동' in td_data.getText():
                        btn_id = f"btn_oddsTooltip{change_num}"
                        layer_id = f"protoOddLayer{change_num}"

                        def _safe_click():
                            # 1) 요소가 존재할 때까지 대기 (클릭 가능까진 말고, 존재/보이기 우선)
                            btn = WebDriverWait(browser, 5).until(
                                EC.presence_of_element_located((By.ID, btn_id))
                            )
                            # 2) 뷰포트로 스크롤
                            try:
                                browser.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
                                time.sleep(0.2)
                            except Exception:
                                pass
                            # 3) 일반 클릭 → 안 되면 JS 클릭
                            try:
                                WebDriverWait(browser, 5).until(EC.element_to_be_clickable((By.ID, btn_id)))
                                btn.click()
                            except Exception:
                                browser.execute_script("document.getElementById(arguments[0]).click();", btn_id)

                        # 열기 시도
                        try:
                            _safe_click()
                        except Exception as e:
                            print(f"[odds toggle] 버튼 열기 실패: {e}")
                            # 버튼이 아예 없거나 DOM 구조가 다르면 스킵
                            continue

                        # 레이어가 표시될 때까지 대기
                        try:
                            WebDriverWait(browser, 5).until(
                                EC.visibility_of_element_located((By.ID, layer_id))
                            )
                        except Exception as e:
                            print(f"[odds layer] 표시 대기 실패: {e}")
                            # 열기 실패면 닫기 재시도 없이 스킵
                            continue

                        # 레이어 파싱
                        odds = browser.find_element(By.ID, layer_id).text
                        change_tr = browser.find_element(By.ID, layer_id).find_elements(By.TAG_NAME, 'tr')
                        odds_num = 0

                        for change_td in change_tr:
                            changed_odds = {}
                            data_exist = {}
                            change_text = change_td.text
                            if '차' in change_text:
                                change_text = change_text.replace(" 차", "차")

                            changed_odds_arr = change_text.split()

                            if game_type == "핸디캡" and odds_num > 0:
                                changed_odds = {
                                    "num": changed_odds_arr[0],
                                    "type": changed_odds_arr[1],
                                    "feature": changed_odds_arr[2],
                                    "win": changed_odds_arr[3],
                                    "draw": changed_odds_arr[4],
                                    "lose": changed_odds_arr[5]
                                }
                                data_exist = {
                                    "gmId": gmId,
                                    "gmTs": gmTs,
                                    "num": int(change_num),
                                    "changed_odds.type": changed_odds_arr[1],
                                    "changed_odds.feature": changed_odds_arr[2],
                                    "changed_odds.win": changed_odds_arr[3],
                                    "changed_odds.draw": changed_odds_arr[4],
                                    "changed_odds.lose": changed_odds_arr[5]
                                }
                            elif game_type == "언더오버" and odds_num > 0:
                                changed_odds = {
                                    "num": changed_odds_arr[0],
                                    "type": changed_odds_arr[1],
                                    "feature": changed_odds_arr[2],
                                    "under": changed_odds_arr[3],
                                    "over": changed_odds_arr[4]
                                }
                                data_exist = {
                                    "gmId": gmId,
                                    "gmTs": gmTs,
                                    "num": int(change_num),
                                    "changed_odds.type": changed_odds_arr[1],
                                    "changed_odds.feature": changed_odds_arr[2],
                                    "changed_odds.under": changed_odds_arr[3],
                                    "changed_odds.over": changed_odds_arr[4]
                                }
                            elif game_type == "일반":
                                if odds_num > 0:
                                    changed_odds = {
                                        "num": changed_odds_arr[0],
                                        "type": "일반",
                                        "win": changed_odds_arr[1],
                                        "draw": changed_odds_arr[2],
                                        "lose": changed_odds_arr[3]
                                    }
                                    data_exist = {
                                        "gmId": gmId,
                                        "gmTs": gmTs,
                                        "num": int(change_num),
                                        "changed_odds.type": "일반",
                                        "changed_odds.win": changed_odds_arr[1],
                                        "changed_odds.draw": changed_odds_arr[2],
                                        "changed_odds.lose": changed_odds_arr[3]
                                    }
                            else:
                                odds_num += 10000
                                continue

                            if odds_num > 0:
                                changed_exist = proto_col.find_one(data_exist)
                                if changed_exist is None:
                                    if changed_odds_arr[0] == "1차" and proto_col_data:
                                        date_string = proto_col_data['created_at'] + datetime.timedelta(hours=9)
                                        date_string = date_string.strftime('%Y-%m-%d %H:%M:%S')
                                        format_ = '%Y-%m-%d %H:%M:%S'
                                        dt_strptime = datetime.datetime.strptime(date_string, format_)
                                        changed_odds.update({"changed_at": proto_col_data['odds_changed_at']})
                                    else:
                                        changed_odds.update({"changed_at": dt.now(tz=KST).strftime('%Y-%m-%d %H:%M:%S')})
                                    if 'win' in changed_odds and changed_odds['win'] == "H":
                                        pass
                                    elif 'win' in changed_odds and changed_odds['win'] == "U/O":
                                        pass
                                    else:
                                        proto_col.update_one({"gmId": gmId, "gmTs": gmTs, "num": int(change_num)},
                                                             {"$push": {"changed_odds": changed_odds}},
                                                             upsert=True)
                            odds_num += 1
                            debug_db_data("8 처리 후", db_data)

                        # 닫기 (토글 버튼 다시 클릭)
                        try:
                            _safe_click()
                        except Exception:
                            pass

            total_count += 1

        lambda_protos_col.update_one({"gmId": gmId, "gmTs": gmTs}, {"$set": {"total_count": total_count, "updated_at": dt.now()}}, upsert=True)
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
            'statusCode': 200,
            'body': json.dumps(body)
        }
    finally:
        try:
            browser.quit()
        except Exception:
            pass
