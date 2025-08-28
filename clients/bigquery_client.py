from google.cloud import bigquery
from google.oauth2 import service_account
import logging, time

logger = logging.getLogger(__name__)

class BigQueryClient:
    def __init__(self, config):
        self.credentials = service_account.Credentials.from_service_account_info(config)
        self.client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id)
        self.project_id = self.credentials.project_id
        
    def create_dataset(self, dataset_id, location="US"):
        dataset_ref = self.client.dataset(dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        
        try:
            dataset = self.client.create_dataset(dataset, timeout=30)
            logger.info(f"Created dataset {self.project_id}.{dataset_id}")
            return dataset
        
        except Exception as e:
            if "Already Exists" in str(e):
                logger.info(f"Dataset {dataset_id} already exists")
                return self.client.get_dataset(dataset_ref)
            else:
                raise e
            
    def list_datasets(self):
        try:
            datasets = list(self.client.list_datasets())
            
            dataset_info = []
            for dataset in datasets:
                dataset_info.append({
                    'dataset_id': dataset.dataset_id
                })
            logger.info(f"Found {len(dataset_info)} datasets")
            return dataset_info
        
        except Exception as e:
            logger.error(f"Failed to list datasets: {e}")
            return []

    def _create_table(self, dataset_id, table_id, schema):
        table_ref = self.client.dataset(dataset_id).table(table_id)
        table = bigquery.Table(table_ref, schema=schema)
        
        try:
            table = self.client.create_table(table)
            logger.info(f"Created table {dataset_id}.{table_id}")
            return table
        
        except Exception as e:
            if "Already Exists" in str(e):
                logger.info(f"Table {dataset_id}.{table_id} already exists")
                return self.client.get_table(table_ref)
            else:
                raise e
            
    def _table_exists(self, dataset_id, table_id):
        """테이블 존재 여부 확인"""
        try:
            table_ref = self.client.dataset(dataset_id).table(table_id)
            self.client.get_table(table_ref)
            return True
        
        except Exception:
            return False
        
    def list_tables_in_dataset(self, dataset_id):
        try:
            tables = list(self.client.list_tables(dataset_id))
            table_info = []
            for table in tables:
                logger.info(dir(table))
                table_info.append({
                    'table_id': table.table_id
                })                
            logger.info(f"Found {len(table_info)} tables in dataset {dataset_id}")
            return table_info
        
        except Exception as e:
            logger.error(f"Failed to list tables in dataset {dataset_id}: {e}")
            return []
    
    def _summarize_errors(self, errors):
        """BigQuery 삽입 에러를 요약하여 반환"""
        error_summary = {}
        for error_item in errors:
            logger.info(error_item)
            if isinstance(error_item, dict) and 'errors' in error_item:
                for err in error_item['errors']:
                    reason = err.get('reason', 'unknown')
                    location = err.get('location', 'unknown')
                    key = f"{reason}:{location}"
                    error_summary[key] = error_summary.get(key, 0) + 1
            elif isinstance(error_item, dict) and 'error' in error_item:
                error_summary['exception'] = error_summary.get('exception', 0) + 1
        return error_summary

    def _get_total_error_summary(self, all_errors):
        """전체 에러 목록에서 에러 타입별 요약 반환"""
        total_error_summary = {}
        for error_item in all_errors:
            if isinstance(error_item, dict) and 'errors' in error_item:
                for err in error_item['errors']:
                    reason = err.get('reason', 'unknown')
                    key = f"{reason}"
                    total_error_summary[key] = total_error_summary.get(key, 0) + 1
            elif isinstance(error_item, dict) and 'error' in error_item:
                total_error_summary['exception'] = total_error_summary.get('exception', 0) + 1
        return total_error_summary
    
    def _insert_rows(self, table_id, rows, batch_size=7000):
        """
        BigQuery에 대량 데이터를 배치 처리로 삽입
        
        Args:
            table_id: 테이블 ID (project.dataset.table 형식)
            rows: 삽입할 데이터 리스트
            batch_size: 배치 크기 (기본값: 1000)
        
        Returns:
            bool: 전체 삽입 성공 여부
        """
        table_ref = self.client.get_table(table_id)
        total_rows = len(rows)
        
        if total_rows == 0:
            logger.info("No rows to insert")
            return True
        
        logger.info(f"Starting batch insert: {total_rows} rows with batch size {batch_size}")
        
        success_count = 0
        error_count = 0
        all_errors = []
        
        # 배치로 나누어 처리
        for i in range(0, total_rows, batch_size):
            batch = rows[i:i + batch_size]
            batch_number = (i // batch_size) + 1
            total_batches = (total_rows + batch_size - 1) // batch_size
            
            try:
                errors = self.client.insert_rows_json(table_ref, batch)
                
                if errors:
                    # 에러 요약 정보만 로깅
                    error_summary = self._summarize_errors(errors)
                    logger.error(f"Batch {batch_number}/{total_batches}: {len(errors)} errors - {error_summary}")
                    all_errors.extend(errors)
                    error_count += len(errors)
                    success_count += len(batch) - len(errors)
                else:
                    logger.info(f"Batch {batch_number}/{total_batches}: Successfully inserted {len(batch)} rows")
                    success_count += len(batch)
                    
            except Exception as e:
                logger.error(f"Batch {batch_number}/{total_batches}: Exception occurred - {str(e)}")
                all_errors.append({"batch": batch_number, "error": str(e)})
                error_count += len(batch)
        
        # 최종 결과 로깅
        logger.info(f"Insert completed: {success_count} successful, {error_count} failed out of {total_rows} total rows")
        
        if all_errors:
            # 전체 에러 요약만 로깅
            total_error_summary = self._get_total_error_summary(all_errors)
            logger.error(f"Total error summary: {total_error_summary}")
            raise Exception(f"BigQuery 삽입 실패: {error_count}개 rows 실패, 에러: {total_error_summary}")
        else:
            logger.info(f"All {total_rows} rows successfully inserted into {table_id}")
            return True
        
    
    def insert_start(self, dataset_id, table_id, schema, rows):
        table_address = f"{self.project_id}.{dataset_id}.{table_id}"

        if not self._table_exists(dataset_id, table_id):
            self._create_table(dataset_id, table_id, schema)
        
        for interval in range(100):
            if self._table_exists(dataset_id, table_id):
                time.sleep(5)
                try:
                    success = self._insert_rows(table_address, rows)
                    if success:
                        return {"status": "success", "message": f"Successfully inserted {len(rows)} rows"}
                except Exception as e:
                    raise Exception(f"BigQuery 데이터 삽입 실패: {str(e)}")
                
            time.sleep(0.3)

        return {"status": "error", "message": "Table creation timeout after 30 seconds"}