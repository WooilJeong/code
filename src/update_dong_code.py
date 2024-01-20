import os
import glob
import json
import time
import shutil
import zipfile
import datetime
import requests
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
from selenium import webdriver

DOWNLOAD_PATH = os.path.join(".", "src", "code")
CHROMEDRIVER_PATH = "C:/chromedriver/chromedriver.exe"


def _extract():
    """
    원천 데이터 추출
    """
    # 다운로드 경로 생성
    DOWNLOAD_PATH_SUB = os.path.join(DOWNLOAD_PATH, "code_dong")
    os.makedirs(DOWNLOAD_PATH, exist_ok=True)
    if os.path.exists(DOWNLOAD_PATH_SUB):
        shutil.rmtree(DOWNLOAD_PATH_SUB)
    os.makedirs(DOWNLOAD_PATH_SUB, exist_ok=True)
    # 크롬드라이버 옵션 지정
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    prefs = {
        "download.default_directory": DOWNLOAD_PATH_SUB,
        "download.directory_upgrade": True,
        "download.prompt_for_download": False,
    }
    options.add_experimental_option('prefs', prefs)
    # 크롬드라이버 정의
    driver = webdriver.Chrome(CHROMEDRIVER_PATH, options=options)
    # URL
    url = "https://www.mois.go.kr/frt/bbs/type001/commonSelectBoardList.do?bbsId=BBSMSTR_000000000052"
    # 드라이버 실행
    driver.get(url)
    time.sleep(1)
    # 검색어 입력
    xpath = '//*[@id="print_area"]/div[1]/form/fieldset/span/input'
    driver.find_element_by_xpath(xpath).send_keys("행정기관(행정동) 및 관할구역(법정동) 변경내역")
    time.sleep(1)
    # 검색 버튼 클릭
    xpath = '//*[@id="print_area"]/div[1]/form/fieldset/span/button'
    driver.find_element_by_xpath(xpath).click()
    time.sleep(1)
    # 최근 게시글 클릭
    xpath = '//*[@id="print_area"]/div[2]/form/table/tbody/tr[1]/td[2]/div/a'
    driver.find_element_by_xpath(xpath).click()
    time.sleep(1)
    # 파일명
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')

    selected_elements = soup.select('h4.subject')
    title_name = selected_elements[0].text

    res = soup.select("#print_area > form > div.table_detail_area > dl.download > dd > div > ul > li")
    res = list(map(lambda x: x.text.replace("\n","").replace("\t",""), res))
    idx = [i for i, e in enumerate(res) if "말소" in e][0] + 1
    time.sleep(1)

    _url = "https://www.mois.go.kr"

    # 파일 URL 찾기
    file_data = soup.select(f"#print_area > form > div.table_detail_area > dl.download > dd > div > ul > li:nth-child({idx}) > a")
    file_link = file_data[0]['href']
    print(file_link)

    file_name = file_data[0].text.strip().split(".zip")[0] + ".zip"
    print(file_name)

    # 파일 다운로드
    file_url = f"{_url}{file_link}"
    r = requests.get(file_url, stream=True)

    file_path = os.path.join(DOWNLOAD_PATH_SUB, file_name)
    with open(file_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk:
                f.write(chunk)

    # 압축 해제
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(DOWNLOAD_PATH_SUB)

    driver.quit()

    # 변환
    법정동코드_경로 = glob.glob(f"{DOWNLOAD_PATH_SUB}*/KIKcd_B*.xlsx")[0]
    행정동코드_경로 = glob.glob(f"{DOWNLOAD_PATH_SUB}*/KIKcd_H*.xlsx")[0]
    혼합코드_경로 = glob.glob(f"{DOWNLOAD_PATH_SUB}*/KIKmix*.xlsx")[0]

    법정동코드 = pd.read_excel(법정동코드_경로, dtype=str)
    행정동코드 = pd.read_excel(행정동코드_경로, dtype=str)
    혼합코드 = pd.read_excel(혼합코드_경로, dtype=str)

    법정동코드['시도코드'] = 법정동코드['법정동코드'].str[:2]
    법정동코드['시군구코드'] = 법정동코드['법정동코드'].str[:5]
    컬럼목록 = ['시도코드','시도명','시군구코드','시군구명','법정동코드','읍면동명','동리명','생성일자','말소일자']
    법정동코드 = 법정동코드[컬럼목록]

    행정동코드['시도코드'] = 행정동코드['행정동코드'].str[:2]
    행정동코드['시군구코드'] = 행정동코드['행정동코드'].str[:5]
    컬럼목록 = ['시도코드','시도명','시군구코드','시군구명','행정동코드','읍면동명','생성일자','말소일자']
    행정동코드 = 행정동코드[컬럼목록]

    혼합코드['시도코드'] = 혼합코드['행정동코드'].str[:2]
    혼합코드['시군구코드'] = 혼합코드['행정동코드'].str[:5]
    컬럼목록 = ['시도코드','시도명','시군구코드','시군구명','행정동코드','읍면동명','법정동코드','동리명','생성일자','말소일자']
    혼합코드 = 혼합코드[컬럼목록]

    # 데이터프레임을 딕셔너리로 변환
    법정동코드_딕셔너리 = {
        "name": title_name,
        "data": 법정동코드.to_dict(),
    }
    행정동코드_딕셔너리 = {
        "name": title_name,
        "data": 행정동코드.to_dict(),
    }
    혼합코드_딕셔너리 = {
        "name": title_name,
        "data": 혼합코드.to_dict(),
    }

    # 데이터프레임을 딕셔너리로 변환 및 JSON 파일 저장
    json_path_bdong = os.path.join(DOWNLOAD_PATH_SUB, "code_bdong.json")
    with open(json_path_bdong, "w") as f:
        f.write(json.dumps(법정동코드_딕셔너리))

    json_path_hdong = os.path.join(DOWNLOAD_PATH_SUB, "code_hdong.json")
    with open(json_path_hdong, "w") as f:
        f.write(json.dumps(행정동코드_딕셔너리))

    json_path_hdong_bdong = os.path.join(DOWNLOAD_PATH_SUB, "code_hdong_bdong.json")
    with open(json_path_hdong_bdong, "w") as f:
        f.write(json.dumps(혼합코드_딕셔너리))

if __name__ == "__main__":
    _extract()