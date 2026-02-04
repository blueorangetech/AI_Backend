import pandas as pd
import logging, re
from typing import Dict, Any

logger = logging.getLogger(__name__)

class DataProcessor:
    """
    클라이언트별 맞춤 데이터 가공 로직을 관리하는 클래스
    """
    
    @staticmethod
    def process_hanssem_report(df: pd.DataFrame) -> pd.DataFrame:
        """
        한샘(Hanssem)용 데이터 가공 로직
        필요한 필드만 추출하고 영문 컬럼명으로 변환합니다.
        """
        logger.info("Hanssem 데이터 가공 프로세스 시작: 필터링 및 영문 변환")
        
        # 1. 컬럼 매핑 정의 (원본: 변경할 영문명)
        column_mapping = {
            'date': 'date',
            '매체': 'media',
            '노출': 'impressions',
            '클릭': 'clicks',
            '소진비용': 'cost',
            '상담신청': 'consultation_requests',
            'UTM_Campaign': 'utm_campaign',
            'UTM_Content': 'utm_content'
        }
        
        # 2. 필수 컬럼 존재 여부 확인
        missing_cols = set(column_mapping.keys()) - set(df.columns)
        if missing_cols:
            raise Exception(f"필수 컬럼이 누락되어 전처리에 실패했습니다: {missing_cols}")
            
        # 3. 선택한 컬럼만 추출 및 즉시 이름 변경
        processed_df = df[list(column_mapping.keys())].rename(columns=column_mapping)
        
        # 4. 데이터 압축 (Aggregation)
        # 지표 데이터와 전환 데이터가 행으로 분리되어 있으므로, 기준 필드로 그룹화하여 합산합니다.
        group_keys = ['date', 'media', 'utm_campaign', 'utm_content']
        numeric_fields = ['impressions', 'clicks', 'cost', 'consultation_requests']
        
        # utm_content를 '_' 기준으로 나눠서 새로운 필드 생성
        if 'utm_content' in processed_df.columns:
            # 문자열로 변환 후 분리 (결측치는 빈 문자열 처리)
            split_series = processed_df['utm_content'].fillna('').astype(str).str.split('_')
            
            # 최대 7개까지 분리하여 필드 생성 (실제 데이터 구조에 따라 조정 가능)
            for i in range(7):
                processed_df[f'utm_content_{i+1}'] = split_series.str[i]
                group_keys.append(f'utm_content_{i+1}')

        # 존재하는 필드만 사용하여 그룹화 및 합산 수행
        active_group_keys = [k for k in group_keys if k in processed_df.columns]
        active_numeric_fields = [n for n in numeric_fields if n in processed_df.columns]
        
        # 수치 데이터의 NaN을 0으로 채움 (합산 오류 방지)
        processed_df[active_numeric_fields] = processed_df[active_numeric_fields].fillna(0)
        
        # 5. 데이터 타입 강제 변환 (Excel -> Pandas 로드 시 float으로 변환되는 것 방지)
        # BigQuery INTEGER 타입에 맞게 정수로 변환해야 00.0 형식을 피할 수 있습니다.
        int_fields = ['impressions', 'clicks', 'consultation_requests']
        for field in int_fields:
            if field in processed_df.columns:
                processed_df[field] = processed_df[field].astype('int64')

        # 날짜 형식 처리: BigQuery DATE 타입에 맞게 YYYY-MM-DD 문자열로 변환
        if 'date' in processed_df.columns:
            try:
                processed_df['date'] = pd.to_datetime(processed_df['date']).dt.strftime('%Y-%m-%d')
            except Exception as e:
                logger.warning(f"날짜 형식 변환 중 오류 발생: {e}")
        
        # 그룹화 및 수치 합산
        processed_df = processed_df.groupby(active_group_keys, as_index=False)[active_numeric_fields].sum()
        
        logger.info(f"Hanssem 데이터 가공 및 압축 완료: {len(processed_df)}행 (UTM 분리 및 그룹화 완료)")
        return processed_df

    @staticmethod
    def process_imweb_inner_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        imweb 내부 데이터 가공 로직
        데이터프레임에서 이상치 제거
        """

         # 정상 형식 패턴: YYYY-MM-DDTHH:MM:SS.000+09:00
        normal_pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}\+\d{2}:\d{2}$'

        # 비정상 형식 행 찾기
        abnormal_indices = []
        for idx, value in enumerate(df['site_owner_join_time']):
            # NaN이 아니고, 정상 패턴에 맞지 않는 경우
            if pd.notna(value) and not re.match(normal_pattern, str(value)):
                abnormal_indices.append(idx)
                print(f"비정상 형식 발견 (행 {idx}): {value}")

        # 비정상 형식 행 제거
        df = df.drop(abnormal_indices)

        logger.info(f"이상치 제거 전 데이터: {len(df)}행")

        # 예시: 특정 컬럼의 특정 값을 가진 행 제거
        # df_cleaned = df[df['your_column'] != 'outlier_value']
        df_cleaned = df.copy()

        logger.info(f"이상치 제거 후 데이터: {len(df_cleaned)}행 (제거된 행: {len(df) - len(df_cleaned)})")

        return df_cleaned
    
    @staticmethod
    def process_default(df: pd.DataFrame) -> pd.DataFrame:
        """기본 가공 로직 (필요시)"""
        return df
