# Timing 서비스 배치
Timing 웹서비스 배치
* 일 기준
    * 매일 국내장 마감 후 종가 기준 DB(PRICE_DAY) 업데이트 
    * 여러 매매기법 기준으로 가격, 거래량 기반 주가 분석
    * 매매기법 공통화 작업 진행(Enum 클래스)
    * 슬랙 봇을 이용한 알림 서비
  
      
* 매매기법
    * Trend Follwing
    * Reversals
    * Triple Screen
    * Dual Mementum

  
* 사용 모듈
  * Finance Data Reader
  * Pandas
  * SqlAlchemy(Python ORM 지원)
---
### 2021.04.20
* readme 작성
* 주가 분석을 위한 테이블 설계
    * MARKET, STOCK_LIST, PRICE_DAY
    * 코드의 하드코딩을 제거하기 위해 ENUM클래스를 이용 
  
---
### 2021.04.22
* 주가 분석 기법을 이용한 스크립트 작성
  * Trend Follwing
  * Reversals
  * Triple Screen
    
---
### 2021.07.20
* 배치 서비스를 봇 서비스로 전환
* 슬랙 봇 생성 및 연결 완료