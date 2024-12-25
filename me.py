import datetime
import glob
import json
import random
import time
import requests
from bs4 import BeautifulSoup
import re
import os
import boto3
from dotenv import load_dotenv
import psycopg2
import schedule
def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print('Error: Creating directory. ' + directory)
def PrintS3FileNames():
    aws_access_key_id = os.getenv('aws_access_key_id')
    aws_secret_access_key = os.getenv('aws_secret_access_key')
    region_name = os.getenv('region_name')
    bucket_name = 'htc-ai-datalake'
    prefix="collection/public-complaint/202412/"
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )

    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            for obj in response['Contents']:
                print(obj['Key'])
        else:
            print("버킷에 파일이 없습니다.")
    except Exception as e:
        print("파일 목록을 가져오는 데 실패했습니다:", str(e))  
def GetSearch():
  categoryList=[
    {
        "categoryName": "사업장 · 건설 · 지정· 폐기물,  최종처분",
        "categoryCode": "0010000000",
        "categoryMean":"사업장폐기물"
    },
    {
        "categoryName": "매립시설,음식물쓰레기,지자체 소각시설 분야",
        "categoryCode": "0020000000",
        "categoryMean":"생활폐기물(음식물류)"
    },
    {
        "categoryName": "폐기물재활용,분리배출표시,EPR제도,재활용촉진 분야",
        "categoryCode": "0030000000",
        "categoryMean":"자원재활용"
    },
    {
        "categoryName": "1회용품규제, 포장폐기물, 폐기물수 · 출입,부담금 분야",
        "categoryCode": "0040000000",
        "categoryMean":"자원순환"
    },
    {
        "categoryName": "대기배출시설,악취,휘발성물질,TMS 분야..",
        "categoryCode": "0050000000",
        "categoryMean":"대기"
    },
    {
        "categoryName": "운행차 배출가스 검사 및 배출가스 저감사업분야",
        "categoryCode": "0180000000",
        "categoryMean":"교통환경"
    },
    {
        "categoryName": "화학물질관리법, 유해화학물질영업허가, 취급시설,",
        "categoryCode": "0170000000",
        "categoryMean":"환경보건"
    },
    {
        "categoryName": "폐수배출시설,배출허용기준,폐수종말처리시설 분야, 축산폐수 처리시설 설치 및 관리, 가축분뇨 관리 분야",
        "categoryCode": "0070000000",
        "categoryMean":"수질관리"
    },
    {
        "categoryName": "비점오염, 수질오염총량제, 낚시금지구역 지정/관리, 물환경측정망 등",
        "categoryCode": "0160000000",
        "categoryMean":"물환경"
    },
    {
        "categoryName": "하수처리장,오수 · 분뇨처리시설,하수도 원인자부담금 분야",
        "categoryCode": "0080000000",
        "categoryMean":"생활하수"
    },
    {
        "categoryName": "전략환경평가, 사전환경영향평가 협의 전 제반사항",
        "categoryCode": "0130000000",
        "categoryMean":"국토환경정책",
    },
    {
        "categoryName": "생활폐기물, 분리배출 관련, 폐기물수입수출 관련",
        "categoryCode": "0090000000",
        "categoryMean":"생활폐기물"
    },
    {
        "categoryName": "화학물질 등록 및 평가 등에 관한법률, 화학물질 수입 관련 분야",
        "categoryCode": "0100000000",
        "categoryMean":"화학물질"
    },
    {
        "categoryName": "국립 · 자연공원관리, 자연공원법, 자연환경해설사",
        "categoryCode": "0110000000",
        "categoryMean":"자연공원"
    },
    {
        "categoryName": "위 분야에 속하지 않는 질의사항(민원실에서 해당과 분류)",
        "categoryCode": "0120000000",
        "categoryMean":"기타"
    }
]
  dataList=[]

  for category in categoryList:
    pageCount=1
    cookies = {
        'JSESSIONID': 'lfSpU2WCa7-PHPZ1-Eamsnp1.euser24',
    }

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        # 'Cookie': 'JSESSIONID=lfSpU2WCa7-PHPZ1-Eamsnp1.euser24',
        'Origin': 'https://www.epeople.go.kr',
        'Referer': 'https://www.epeople.go.kr/frmSub/pttn/openPttnList.npaid',
        'Sec-Fetch-Dest': 'iframe',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }
    while True:
      data = {
          '_csrf': 'ffc73d29-3dff-4c31-bee6-351bec0e831a',
          'recordCountPerPage': '10',
          'pageIndex': str(pageCount),
          'epUnionSn': '',
          'pttnRqstNo': '',
          'instPrcsSttsCd': '',
          'dutySctnNm': '',
          'lcgovBlngInstCd': '1480000',
          'bestInstPttnClsfCd': category['categoryCode'],
          'pttnDrctRcptYn': category['categoryCode'],
          'rcptTySctnCd': category['categoryCode'],
          'chrgDeptCd': category['categoryCode'],
          'searchWordType': '1',
          'searchWord': '',
          'rqstStDt': '2024-06-26',
          'rqstEndDt': '2024-12-23',
          'dateType': '5',
          'focusPerPageYn': '',
          'frm_frmMenuMngNo': 'WBF-0508-000017',
          'frm_instCd': '1480000',
          'frm_clsfAplcYn': 'Y',
          'frm_rcptPathInstCd': '1480000',
          'frm_frmUrlSn': '2',
          'frm_frmAllUrl': 'https://www.epeople.go.kr/frm/pttn/pttnRqstInfo.npaid?frmMenuMngNo=WBF-0508-000017',
          'frm_prcsInstChcePsblYn': 'N',
          'frm_thptInfofAgreYn': 'N',
          'frmDsgnType': '',
          # 'frm_frmMenuClsfCd': '0020000000',
          # 'frm_chrgDeptCd': '1481077',
          # 'frm_pttnDrctRcptYn': 'Y',
          # 'frm_rcptTySctnCd': '9A040001',
          # 'frm_clsfRpsnNm': '매립시설,음식물쓰레기,지자체 소각시설 분야',
          'frm_frmMngrCnt': '0',
          'frm_sysCrtnMngrIdCnt': '0',
          'frm_areaSntlYn': 'N',
          'frm_atchFileSntlYn': 'N',
          'frm_drqstrInfoIndcYn': 'N',
          'frm_dpttnrInfoSntlYn': 'N',
          'frm_pttnSctnCd': '80030001',
          'frm_scryPttnSctnCd': '0A070002',
          'frm_gdncWrdsUseYn': 'N',
          'frm_egppYn': 'N',
          'frm_cnrsYn': 'Y',
          'frm_addnCltnItmUseYn': 'N',
          'frm_pttnClsfCd': '',
          'frm_prcsTmltNumd': '7',
          'frm_basePrcsPrd': '7',
          'frm_menuCd': 'PC001',
          'frm_proPotMngrIdCnt': '0',
          'frm_ablYn': 'N',
          'acesMode': 'frm',
          'frm_lwsInstNm': '환경부',
          'frm_pttnPrcsCaseYn': 'Y',
          'frm_cntnCl': '',
          'frm_pttnKndCd': '',
      }

      response = requests.post('https://www.epeople.go.kr/frmSub/pttn/openPttnList.npaid', cookies=cookies, headers=headers, data=data)
      soup=BeautifulSoup(response.text, 'html.parser')
      # with open('result.html', 'w', encoding='utf-8') as f:
      #   f.write(soup.prettify())
      itemsCheck=soup.find_all('a',attrs={'class':'tit'})
      table=soup.find('table',attrs={'class':'tbl default brd1'})
      items=table.find_all('tr')
      
      print("아이템수",len(itemsCheck))
      if len(itemsCheck)==0:
        print("더없음")
        break
      for index,item in enumerate(items):
        if index==0:
          continue
        try:
          itemCodeRaw=item.find('a')['onclick']
          match = re.findall(r"'(.*?)'", itemCodeRaw)
          if match:
            code1 = match[0]
            code2 = match[1] if len(match) > 1 else None
          print(code1,code2)
          title=item.find('a').get_text()
          caseID=item.find_all('td')[0].get_text().strip()
          data={
            'caseID':caseID,
            'code1':code1,
            'code2':code2,
            'title':title,
            'pageCount':pageCount,
            'categoryName':category['categoryName'],
            'categoryCode':category['categoryCode'],
            'categoryMean':category['categoryMean']
          }
          dataList.append(data)
        except Exception as e:
          print(e)
      with open('dataList.json', 'w', encoding='utf-8') as f:
        json.dump(dataList, f, ensure_ascii=False, indent=4)
      pageCount+=1
      time.sleep(1)
def GetDetail(inputData):
  cookies = {
      'JSESSIONID': 'CSmFwwMU6++NuV7Svob1FBeS.euser13',
  }

  headers = {
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
      'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
      'Cache-Control': 'max-age=0',
      'Connection': 'keep-alive',
      'Content-Type': 'application/x-www-form-urlencoded',
      # 'Cookie': 'JSESSIONID=CSmFwwMU6++NuV7Svob1FBeS.euser13',
      'Origin': 'https://www.epeople.go.kr',
      'Referer': 'https://www.epeople.go.kr/frmSub/pttn/openPttnList.npaid',
      'Sec-Fetch-Dest': 'iframe',
      'Sec-Fetch-Mode': 'navigate',
      'Sec-Fetch-Site': 'same-origin',
      'Sec-Fetch-User': '?1',
      'Upgrade-Insecure-Requests': '1',
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
      'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
      'sec-ch-ua-mobile': '?0',
      'sec-ch-ua-platform': '"Windows"',
  }

  data = {
    '_csrf': '74ab1c17-1441-4927-b06a-35a0bb47ab3b',
    'recordCountPerPage': '10',
    'pageIndex': '1',
    'epUnionSn': '',
    'pttnRqstNo': inputData['code1'],
    'instPrcsSttsCd': inputData['code2'],
    'dutySctnNm': '',
    'lcgovBlngInstCd': '1480000',
    'bestInstPttnClsfCd': inputData['categoryCode'],
    'pttnDrctRcptYn': inputData['categoryCode'],
    'rcptTySctnCd': inputData['categoryCode'],
    'chrgDeptCd': inputData['categoryCode'],
    'searchWordType': '1',
    'searchWord': '',
    'rqstStDt': '2024-06-27',
    'rqstEndDt': '2024-12-24',
    'dateType': '0',
    'focusPerPageYn': '',
    'frm_frmMenuMngNo': 'WBF-0508-000017',
    'frm_instCd': '1480000',
    'frm_clsfAplcYn': 'Y',
    'frm_rcptPathInstCd': '1480000',
    'frm_frmUrlSn': '2',
    'frm_frmAllUrl': 'https://www.epeople.go.kr/frm/pttn/pttnRqstInfo.npaid?frmMenuMngNo=WBF-0508-000017',
    'frm_prcsInstChcePsblYn': 'N',
    'frm_thptInfofAgreYn': 'N',
    'frmDsgnType': '',
    'frm_frmMenuClsfCd': '0010000000',
    'frm_chrgDeptCd': '1481065',
    'frm_pttnDrctRcptYn': 'Y',
    'frm_rcptTySctnCd': '9A040001',
    'frm_clsfRpsnNm': '사업장 · 건설 · 지정· 폐기물,  최종처분',
    'frm_frmMngrCnt': '0',
    'frm_sysCrtnMngrIdCnt': '0',
    'frm_areaSntlYn': 'N',
    'frm_atchFileSntlYn': 'N',
    'frm_drqstrInfoIndcYn': 'N',
    'frm_dpttnrInfoSntlYn': 'N',
    'frm_pttnSctnCd': '80030001',
    'frm_scryPttnSctnCd': '0A070002',
    'frm_gdncWrdsUseYn': 'N',
    'frm_egppYn': 'N',
    'frm_cnrsYn': 'Y',
    'frm_addnCltnItmUseYn': 'N',
    'frm_pttnClsfCd': '',
    'frm_prcsTmltNumd': '7',
    'frm_basePrcsPrd': '7',
    'frm_menuCd': 'PC001',
    'frm_proPotMngrIdCnt': '0',
    'frm_ablYn': 'N',
    'acesMode': 'frm',
    'frm_lwsInstNm': '환경부',
    'frm_pttnPrcsCaseYn': 'Y',
    'frm_cntnCl': '',
    'frm_pttnKndCd': '',
}

  response = requests.post(
      'https://www.epeople.go.kr/frmSub/pttn/openPttnListView.npaid',
      cookies=cookies,
      headers=headers,
      data=data,
  )
  soup=BeautifulSoup(response.text, 'html.parser')
  with open('result.html', 'w', encoding='utf-8') as f:
    f.write(soup.prettify())
  askTitle=soup.find('div',attrs={'class':'samC_top'}).get_text().strip()
  askContent=soup.find('div',attrs={'class':'samC_c'}).get_text().strip().split("\r")
  askDate=soup.find('span',attrs={'class':'samC_date'}).get_text().strip()
  # print(askTitle,askContent,askDate)
  
  # answerTitle=soup.find('div',attrs={'class':'samC_top'}).get_text()
  # Replace <br> tags with newline characters
  for br in soup.find_all('br'):
      br.replace_with('\n')
  
  answerDate = soup.find_all('span', attrs={'class': 'samC_date'})[-1].get_text().strip()

  answerContent = soup.find_all('div', attrs={'class': 'samC_top'})[-1]
  # Decompose the span element with class 'samC_date' from answerContent
  for span in answerContent.find_all('span', attrs={'class': 'samC_date'}):
      span.decompose()
  answerContent=answerContent.get_text().strip().split("\n")
  department=soup.find('ul',attrs={'class':'samC_info'}).find_all('dd')[0].get_text().strip()
  relatedLaws=soup.find('ul',attrs={'class':'samC_info'}).find_all('dd')[1].get_text().strip()
  # print(answerContent,answerDate)
  baseData={
    "ME-QA": [
        {
            "metadata": {
                "Type": "Me-qa",
                "Source": "http://www.me.go.kr/home/web/index.do?menuId=10243",
                "Author": "환경부",
                "CreationDate": "",
                "ModDate": "",
                "Category": "",
                "FileName": ""
            },
            "data": {
                "caseID": "",
                "caseTitle": "",
                "caseContent": [
                    ],
                "questionDate": "",
                "answer": [],
                "answerDate": "",
                "status": "",
                "department": "",
                "relatedLaws": ""
            }
        }
    ]
  }
  timeNow=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  timeNowDay=datetime.datetime.now().strftime("%Y%m%d")
  baseData['ME-QA'][0]['metadata']['CreationDate']=timeNow
  baseData['ME-QA'][0]['metadata']['ModDate']=timeNow
  baseData['ME-QA'][0]['metadata']['Category']="QNA{}".format(inputData['categoryCode'])
  def sanitize_filename(filename):
    # 파일명에서 허용되지 않는 특수문자 제거
    # Windows와 Unix 시스템 모두에서 안전하게 사용할 수 있는 파일명으로 변환
    invalid_chars = r'[<>:"/\\|?*\n\r\t]'
    sanitized = re.sub(invalid_chars, '', filename)
    # 추가적인 특수문자 및 공백 처리
    sanitized = re.sub(r'[\s]+', '_', sanitized)  # 공백을 언더스코어로 변환
    sanitized = re.sub(r'[^\w\-_.]', '', sanitized)  # 알파벳, 숫자, 하이픈, 언더스코어, 점만 허용
    return sanitized

  # 파일명 생성 부분
  safe_title = sanitize_filename(inputData['title'])
  safe_category = sanitize_filename(inputData['categoryMean'])
  baseData['ME-QA'][0]['metadata']['FileName'] = f"{safe_category}_{safe_title}_{timeNowDay}"
  baseData['ME-QA'][0]['data']['caseID']=inputData['caseID']
  baseData['ME-QA'][0]['data']['caseTitle']=inputData['title']
  baseData['ME-QA'][0]['data']['caseContent']=askContent
  baseData['ME-QA'][0]['data']['questionDate']=askDate
  baseData['ME-QA'][0]['data']['answer']=answerContent
  baseData['ME-QA'][0]['data']['answerDate']=answerDate
  baseData['ME-QA'][0]['data']['department']=department
  baseData['ME-QA'][0]['data']['relatedLaws']=relatedLaws
  baseData['ME-QA'][0]['data']['status']='완료'
  
  # data={
  #   'caseID':inputData['code1'],
  #   'askTitle':askTitle,
  #   'askContent':askContent,
  #   'askDate':askDate,
  #   'answerContent':answerContent,
  #   'answerDate':answerDate,
  #   'department':department,
  #   'relatedLaws':relatedLaws,
  #   'status':'완료'
  # }
  with open('result/{}.json'.format(baseData['ME-QA'][0]['metadata']['FileName']), 'w', encoding='utf-8') as f:
    json.dump(baseData, f, ensure_ascii=False, indent=4)
  return baseData
def UploadImageToS3(inputData):
  # AWS 계정의 액세스 키와 시크릿 키를 설정합니다.
  timeNow=datetime.datetime.now().strftime("%Y%m")
  aws_access_key_id=os.getenv('aws_access_key_id')
  aws_secret_access_key=os.getenv('aws_secret_access_key')
  region_name=os.getenv('region_name')
  bucket_name='htc-ai-datalake'
  
  # S3 클라이언트를 생성합니다.
  s3_client = boto3.client(
      's3',
      aws_access_key_id=aws_access_key_id,
      aws_secret_access_key=aws_secret_access_key,
      region_name=region_name
  )
  
  
  try:
      response = s3_client.upload_file(
          Filename="result/{}.json".format(inputData['ME-QA'][0]['metadata']['FileName']),
          Bucket=bucket_name,
          Key=f"collection/public-complaint/{timeNow}/{inputData['ME-QA'][0]['metadata']['FileName']}.json"  # timeNow(예: 202412) 폴더 안에 파일 저장
      )
      print("JSON파일 업로드 성공!")
  except Exception as e:
      print("파일 업로드 실패:", str(e))
      return None 
def insert_dummy_data(inputData):
    connection = None
    cursor = None
    try:
        initial_db_params = {
            'dbname': 'htc-aikr-prod',
            'user': 'postgres',
            'password': 'ddiMaster1!',
            'host': '127.0.0.1',
            'port': '5432'
        }
        
        connection = psycopg2.connect(**initial_db_params)
        cursor = connection.cursor()
        
        insert_query = """
            INSERT INTO "COLLECTION_DATA" ("NAME", "FILE_NAME", "FILE_PATH", "METHOD", "COLLECTION_ID")
            VALUES (%s, %s, %s, %s, %s)
        """
        
        datas = []
        timeNow = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        timeNowMonth = datetime.datetime.now().strftime("%Y%m")
        koreanTitle = inputData['ME-QA'][0]['data']['caseTitle']
        attachmentFileName = inputData['ME-QA'][0]['metadata']['FileName']
        attachmentFileUrl = inputData['ME-QA'][0]['metadata']['FileName']
        datas.append((koreanTitle, attachmentFileName, attachmentFileUrl, 'AUTO', "108"))
        
        for data in datas:
            cursor.execute(insert_query, data)
        
        connection.commit()
        print("Dummy data inserted successfully.")

    except psycopg2.Error as error:
        print(f"Database error: {error}")
        if connection:
            connection.rollback()
    except Exception as error:
        print(f"Error: {error}")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
def job():
  GetSearch()

  with open('dataList.json', 'r', encoding='utf-8') as f:
    dataList = json.load(f)
  for index,data in enumerate(dataList):
    print("{}/{}번째 확인중...".format(index+1,len(dataList)))
    baseData=GetDetail(data)
    UploadImageToS3(baseData)
    insert_dummy_data(baseData)
    time.sleep(random.randint(1,3))
    
    files = glob.glob('result/*')
    for f in files:
        try:
            os.remove(f)
            print(f"{f} 삭제 성공")
        except Exception as e:
            print(f"{f} 삭제 실패: {str(e)}")
    # break

schedule.every().monday.at("09:00").do(job)

print("최초실행")
job()

while True:
    print("현재시간은",datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    schedule.run_pending()
    time.sleep(60)


#=======파일 존재 확인하기
# load_dotenv()
# PrintS3FileNames()