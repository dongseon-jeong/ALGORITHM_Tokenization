# 리뷰 토큰화

## 토큰화 목적
리뷰를 토큰 단위로 분해하여 주요 키워드 마다 빈도수가 높은 토큰을 집계하거나 워드클라우드로 시각화하여
어느 키워드에 어떤 의견이 많은 지 파악  

**예시**  
길이 : 짧다, 길다

## 유의 사항
- 형태소는 의미를 내포하는 명사, 동사, 형용사에 한정
- 데이터양이 많아서 한정된 리소스로 빠른 결과를 출력하기 위해 병렬로 연산
- 리뷰 전체 부분을 토큰화하는 것은 비효율 적이라서 각 주요 키워드의 하이라이트 부분만 추출하여 토큰화 진행
하이라이트 부분은 문장 내 인덱스로만 저장되어 있음  
- 유사 단어를 하나의 형태로 변형하는 과정이 필요함  
  **예시**  
  '좋아서' , '좋은데' , '좋다' , '좋았어요' > 최종 토큰은 '좋"과 같은 형태로 분해되어 대시보드에서 최종 형태로 변형 > 최종 노출 "좋다"

## 토크나이저 선택
**R konlp mecab** : 정확도도 크게 나쁘지 않고 토큰화가 가장 빠르고 사용자 단어 추가 가능


## NEXT STEP
- 부정의 의미라도 형태소 분석 후 집계할 경우 긍정으로 집계됨  
  
  **예시**  
  "길지 않다" > "길다" , "않다" 로 각각의 형태소 분해되어 최종 "길다"로 집계  
- 여러 키워드의 하이라이트가 겹칠 경우 특정 키워드에 다른 키워드의 형태소가 포함됨  
  
  **예시**  
  "색상과 디자인 모두 맘에 들어요"의 리뷰 하이라이트가 있을 경우 하이라이트가 겹쳐서 색상과 디자인에서 각각 중복 집계
- 한번에 연산 가능한 리뷰수가 제한되어서 배치 데이터 생성이 필요함  