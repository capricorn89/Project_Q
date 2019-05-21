# Factor Exposed Pair Trading
Sector를 닫아놓은 상태로 Size, Value 등의 팩터에 노출하는 Pair Trading 전략

# Contents 
1.  전체 종목 유니버스 : KSP + KSQ
2.  평균 시총 및 거래대금 기준 미달, 관리종목, 거래정지 등 편입 불가 종목 제외
3.  섹터 내에서 Pair 선정
    -   WICS 기준
4.  Pair Universe는 (대형주, 소형주) 혹은 (저Value, 고Value) 와 같은 형태로 구성
    -   대형주 / 소형주를 구분할 기준
    -   고Value, 저Value 를 구분할 기준
5.  Cointegration 검정
    -   얼마동안의 데이터로 공적분을 검정할 건지
    -   p-value 기준은 얼마로 잡을지?
6.  Cointegrated Pair 에서 스프레드가 일정 범위 벗어난 경우 진입, 들어오면 청산. 많이 벗어나면 Loss-cut
    -   진입 구간 설정
    -   청산 구간 설정
    -   손절 구간 설정
7.  Pairs Long, K200 Short 의 월별 리밸런싱 수익률 확인