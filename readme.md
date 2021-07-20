# Timing 서비스 배치
Timing 웹서비스 배치
* 일 기준
    * 매일 국내, 미국 장 마감 후 종가 기준 DB(ST_PRICE_DAY) 업데이트 
    * 여러 매매기법 기준으로 가격, 거래량 기반 주가 분석
    * 매매기법 공통화(MA_TRADE_WAY)
    * 공통 테이블 loop -> 파일명 찾아 인스턴스 생성 후 분석  
   

* 실시간 기준
    * 유저 즐겨찾기 기반 group by된 종목 코드만 실시간 분석 진행  
  
      
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
    * MA_TRADE_WAY: TRADE_CLS_CD, TRADE_CLS_NM, TIMING_PERIOD, SAFE_DEGREE, PY_BATCH_NAME, USE_YN, LAST_UPDATE
    * SAFE_DEGREE 와 TIMING_PERIOD 는 유저별 특성을 위해 부여
    * 코드의 하드코딩을 제거하기 위해 PY_BATCH_NAME 작성(테이블로 배치를 관리) 
  
---
### 2021.04.22
* 공통코드 데이터 관리 TABLE버전 외 ENUM 클래스로 관리하는 버전 신규생성
  * 오히려 코드가 간견해지고, 히스토리 관리가 가능한 ENUM 클래스를 사용하는 것으로 변경
  * 참고를 위해 테이블 관리버전은 updateDayTable로 디렉토리를 변경해 저장
    
### 2021.07.20
* 배치 서비스를 봇 서비스로 전환
* 슬랙 봇 생성 및 연결 완료