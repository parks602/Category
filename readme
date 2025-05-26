# 쿠팡 카테고리 분류 파이프라인 정리

## STEP 1. 새로운 학습용 데이터셋 확보

* **데이터 소스**: `retargeting_event_storage_orc` 테이블의 `rt_cd = 300` (쿠팡 데이터)
* **수집 범위**: 전월 기준 1일부터 4일 간격으로 약 8일간 수집 (총 약 4천만 건)
* **작업 내용**: 쿼리 실행, CSV 저장
* **생성 파일**:
    * `/pkw/coupang_category/data/YYYY_MM/raw/YYYYMMDD.csv` (일별 쿠팡 데이터)

***

## STEP 2. 추가된 카테고리 확인

* **작업 내용**:
    * 작업 월 카테고리의 unique 값 확인
    * 기준 월의 카테고리 사전과 비교하여 새로운 카테고리 확인 및 저장
* **생성 파일**:
    * `/pkw/coupang_category/data/YYYY_MM/new_unique.csv` (작업 월 unique 카테고리)
    * `/pkw/coupang_category/data/YYYY_MM/diff_data.csv` (기준 월 대비 추가된 카테고리)
* **참고 파일**:
    * `/pkw/coupang_category/data/yyyy-mm/updated_unique.csv` (기준 월 카테고리 사전)

***

## STEP 3. 추가된 카테고리의 이름 크롤링

* **작업 내용**:
    * `diff_data.csv` 내 추가된 카테고리의 `product_id`를 통해 Depth 1\~5의 `category_name` 크롤링
* **생성 파일**:
    * `/pkw/coupang_category/data/YYYY_MM/coupang_cate_all.csv` (작업 월 카테고리 사전)
* **참고 파일**:
    * 기준 월 카테고리 사전: `/pkw/coupang_category/data/yyyy-mm/coupang_cate_all.csv`
    * 추가된 카테고리 목록: `/pkw/coupang_category/data/YYYY_MM/diff_data.csv`

***

## STEP 4. 학습 데이터 정제

* **작업 내용**:
    * Step 3에서 수집한 카테고리 사전을 기반으로 raw 데이터의 카테고리 정제
    * 정제 기준:
        * `cate1_name`이 없는 경우 → `null` 처리
        * 소수 category4는 `category1` 기준으로 통합
* **생성 파일**:
    * `/pkw/coupang_category/data/YYYY_MM/define/YYYYMMDD.csv` (정제된 일별 데이터)
    * `/pkw/coupang_category/data/YYYY_MM/cate_dictionary.csv` (DB 적재용 카테고리 사전)
* **참고 파일**:
    * Raw 데이터: `/pkw/coupang_category/data/YYYY_MM/raw/YYYYMMDD.csv`
    * 카테고리 사전: `/pkw/coupang_category/data/YYYY_MM/coupang_cate_all.csv`

***

## STEP 5. 카테고리 사전 DB 적재

* **적재 대상 테이블**: `rt_category_info_coupang`
* **테이블 정의 예시**:

```sql
CREATE TABLE IF NOT EXISTS rt_category_info_coupang (
  lv1_code int,
  lv2_code int,
  lv3_code int,
  lv4_code int,
  lv1_name string,
  lv2_name string,
  lv3_name string,
  lv4_name string
)
PARTITIONED BY (day string)
STORED AS PARQUET
LOCATION 'hdfs://ha-cluster/rt_category_info_coupang/logs'
```

* **참조 데이터**:
    * `/pkw/coupang_category/data/YYYY_MM/cate_dictionary.csv`

***

## STEP 6. 학습 데이터 생성 및 BPE 분류기 훈련

* **작업 내용**:
    * 정제된 쿠팡 데이터를 활용해 학습 데이터 구성
    * BPE 기반 카테고리 분류기 학습
* **생성 파일**:
    * `/pkw/coupang_category/data/YYYY_MM/coupang_train_xxxx_YYYY_MM.csv` (학습 데이터)
    * `/pkw/coupang_category/data/YYYY_MM/btm_percent_dict_YYYY_MM.pickle` (BPE 모델)
* **참조 데이터**:
    * `/pkw/coupang_category/data/YYYY_MM/define/YYYYMMDD.csv`
