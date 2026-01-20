from google.cloud import bigquery
from utils.bigquery_client_manager import get_bigquery_client
import logging, time, io
import pandas as pd

logger = logging.getLogger(__name__)


class BigQueryClient:
    def __init__(self, config):
        self.config = config
        self._client = None

    async def _get_client(self):
        """BigQuery 클라이언트 반환 (매니저를 통해)"""
        if self._client is None:
            self._client = await get_bigquery_client(self.config)
        return self._client

    async def create_dataset(self, dataset_id, location="US"):
        client = await self._get_client()
        dataset_ref = client.dataset(dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location

        try:
            dataset = client.create_dataset(dataset, timeout=30)
            logger.info(f"Created dataset {client.project}.{dataset_id}")
            return dataset

        except Exception as e:
            if "Already Exists" in str(e):
                logger.info(f"Dataset {dataset_id} already exists")
                return client.get_dataset(dataset_ref)
            else:
                raise e

    async def list_datasets(self):
        try:
            client = await self._get_client()
            datasets = list(client.list_datasets())

            dataset_info = []
            for dataset in datasets:
                dataset_info.append({"dataset_id": dataset.dataset_id})
            logger.info(f"Found {len(dataset_info)} datasets")
            return dataset_info

        except Exception as e:
            logger.error(f"Failed to list datasets: {e}")
            return f"Failed to list datasets: {e}"
    
    async def list_tables(self, dataset_id):
        """테이블 목록 조회"""
        try:
            client = await self._get_client()
            dataset_ref = client.dataset(dataset_id)
            tables = client.list_tables(dataset_ref)

            table_info = []
            for table in tables:
                table_info.append({"table_id": table.table_id})
            
            return table_info
        
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            return f"Failed to list tables: {e}"
    
    async def get_table_schema(self, dataset_id, table_id):
        """테이블 스키마 조회"""
        try:
            client = await self._get_client()
            table_ref = client.dataset(dataset_id).table(table_id)
            table = client.get_table(table_ref)
            return table.schema
        
        except Exception as e:
            logger.error(f"Failed to get table schema: {e}")
            return f"Failed to get table schema: {e}"
    
    async def execute_bigquery_sql(self, sql: str):
        """BigQuery SQL 쿼리 실행"""
        try:
            client = await self._get_client()
            query_job = client.query(sql)
            results = query_job.result()
            
            final_data = []
            for row in results:
                final_data.append(dict(row)) # 각 행을 dict로 변환

            return {"data": final_data}
        
        except Exception as e:
            logger.error(f"Failed to execute SQL: {e}")
            return f"Failed to execute SQL: {e}"
    

    async def _create_table(self, dataset_id, table_id, schema):
        client = await self._get_client()
        table_ref = client.dataset(dataset_id).table(table_id)
        table = bigquery.Table(table_ref, schema=schema)

        try:
            table = client.create_table(table)
            logger.info(f"Created table {dataset_id}.{table_id}")
            return table

        except Exception as e:
            if "Already Exists" in str(e):
                logger.info(f"Table {dataset_id}.{table_id} already exists")
                return client.get_table(table_ref)
            else:
                raise e

    async def _table_exists(self, dataset_id, table_id):
        """테이블 존재 여부 확인"""
        try:
            client = await self._get_client()
            table_ref = client.dataset(dataset_id).table(table_id)
            client.get_table(table_ref)
            return True

        except Exception:
            return False

    async def check_date_exists(self, dataset_id, table_id, insert_date, date_field="date"):
        """특정 날짜의 데이터가 이미 존재하는지 확인"""
        if not await self._table_exists(dataset_id, table_id):
            return False

        try:
            client = await self._get_client()
            query = f"""
            SELECT COUNT(*) as count
            FROM `{client.project}.{dataset_id}.{table_id}`
            WHERE {date_field} = DATE('{insert_date}')
            """

            query_job = client.query(query)
            results = query_job.result()

            for row in results:
                return row.count > 0

        except Exception as e:
            logger.warning(f"Failed to check date existence: {str(e)}")
            return False

        return False

    async def delete_data_by_date(self, dataset_id, table_id, delete_date):
        """특정 날짜의 데이터를 삭제"""
        if not await self._table_exists(dataset_id, table_id):
            logger.info(f"Table {dataset_id}.{table_id} does not exist, skip deletion")
            return True

        try:
            client = await self._get_client()
            query = f"""
            DELETE FROM `{client.project}.{dataset_id}.{table_id}`
            WHERE date = DATE('{delete_date}')
            """

            logger.info(f"Deleting data for date {delete_date} from {dataset_id}.{table_id}")
            query_job = client.query(query)
            query_job.result()  # 쿼리 완료 대기

            logger.info(f"Successfully deleted data for date {delete_date}")
            return True

        except Exception as e:
            logger.error(f"Failed to delete data for date {delete_date}: {str(e)}")
            raise Exception(f"BigQuery 데이터 삭제 실패: {str(e)}")

    async def delete_data_by_date_range(self, dataset_id, table_id, start_date, end_date):
        """날짜 범위의 데이터를 삭제"""
        if not await self._table_exists(dataset_id, table_id):
            logger.info(f"Table {dataset_id}.{table_id} does not exist, skip deletion")
            return True

        try:
            client = await self._get_client()
            query = f"""
            DELETE FROM `{client.project}.{dataset_id}.{table_id}`
            WHERE date >= DATE('{start_date}') AND date <= DATE('{end_date}')
            """

            logger.info(f"Deleting data from {start_date} to {end_date} from {dataset_id}.{table_id}")
            query_job = client.query(query)
            query_job.result()  # 쿼리 완료 대기

            logger.info(f"Successfully deleted data from {start_date} to {end_date}")
            return True

        except Exception as e:
            logger.error(f"Failed to delete data for date range {start_date} - {end_date}: {str(e)}")
            raise Exception(f"BigQuery 날짜 범위 데이터 삭제 실패: {str(e)}")

    async def truncate_table(self, dataset_id, table_id):
        """테이블의 모든 데이터를 삭제 (TRUNCATE)"""
        if not await self._table_exists(dataset_id, table_id):
            logger.info(f"Table {dataset_id}.{table_id} does not exist, skip truncate")
            return True

        try:
            client = await self._get_client()
            query = f"""
            TRUNCATE TABLE `{client.project}.{dataset_id}.{table_id}`
            """

            logger.info(f"Truncating table {dataset_id}.{table_id}")
            query_job = client.query(query)
            query_job.result()  # 쿼리 완료 대기

            logger.info(f"Successfully truncated table {dataset_id}.{table_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to truncate table {dataset_id}.{table_id}: {str(e)}")
            raise Exception(f"BigQuery 테이블 truncate 실패: {str(e)}")

    async def list_tables_in_dataset(self, dataset_id):
        try:
            client = await self._get_client()
            tables = list(client.list_tables(dataset_id))
            table_info = []
            for table in tables:
                logger.info(dir(table))
                table_info.append({"table_id": table.table_id})
            logger.info(f"Found {len(table_info)} tables in dataset {dataset_id}")
            return table_info

        except Exception as e:
            logger.error(f"Failed to list tables in dataset {dataset_id}: {e}")
            return []

    def _summarize_errors(self, errors):
        """BigQuery 삽입 에러를 요약하여 반환"""
        error_summary = {}
        for error_item in errors:
            if isinstance(error_item, dict) and "errors" in error_item:
                for err in error_item["errors"]:
                    reason = err.get("reason", "unknown")
                    location = err.get("location", "unknown")
                    key = f"{reason}:{location}"
                    error_summary[key] = error_summary.get(key, 0) + 1
            elif isinstance(error_item, dict) and "error" in error_item:
                error_summary["exception"] = error_summary.get("exception", 0) + 1
        return error_summary

    def _get_total_error_summary(self, all_errors):
        """전체 에러 목록에서 에러 타입별 요약 반환"""
        total_error_summary = {}
        for error_item in all_errors:
            if isinstance(error_item, dict) and "errors" in error_item:
                for err in error_item["errors"]:
                    reason = err.get("reason", "unknown")
                    key = f"{reason}"
                    total_error_summary[key] = total_error_summary.get(key, 0) + 1
            elif isinstance(error_item, dict) and "error" in error_item:
                total_error_summary["exception"] = (
                    total_error_summary.get("exception", 0) + 1
                )
        return total_error_summary

    async def _insert_rows(self, table_id, rows):
        """
        BigQuery에 로드 방식으로 데이터 삽입

        Args:
            table_id: 테이블 ID (project.dataset.table 형식)
            rows: 삽입할 데이터 리스트

        Returns:
            bool: 삽입 성공 여부
        """
        client = await self._get_client()
        table_ref = client.get_table(table_id)
        total_rows = len(rows)

        if total_rows == 0:
            logger.info("No rows to insert")
            return True

        logger.info(f"Starting load: {total_rows} rows")

        try:
            # DataFrame으로 변환
            df = pd.DataFrame(rows)

            # NDJSON 형식으로 변환
            ndjson_data = df.to_json(orient='records', lines=True, date_format='iso')

            # 메모리 파일 객체 생성 (bytes로 변환)
            file_obj = io.BytesIO(ndjson_data.encode('utf-8'))

            # 로드 작업 설정
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                autodetect=False,  # 스키마 자동 감지 비활성화
                ignore_unknown_values=True  # 알 수 없는 필드 무시
            )

            # 로드 작업 실행
            job = client.load_table_from_file(
                file_obj, table_ref, job_config=job_config
            )

            # 작업 완료 대기
            job.result()

            logger.info(f"Successfully loaded {total_rows} rows into {table_id}")
            return True

        except Exception as e:
            logger.error(f"Load job failed - {str(e)}")
            raise Exception(f"BigQuery 로드 실패: {str(e)}")

    async def insert_start(self, dataset_id, table_id, schema, rows):
        client = await self._get_client()
        table_address = f"{client.project}.{dataset_id}.{table_id}"

        if not await self._table_exists(dataset_id, table_id):
            await self._create_table(dataset_id, table_id, schema)

        for interval in range(100):
            if await self._table_exists(dataset_id, table_id):
                time.sleep(5)
                try:
                    success = await self._insert_rows(table_address, rows)
                    if success:
                        return {
                            "status": "success",
                            "message": f"Successfully inserted {len(rows)} rows",
                        }
                except Exception as e:
                    raise Exception(f"BigQuery 데이터 삽입 실패: {str(e)}")

            time.sleep(0.3)

        return {"status": "error", "message": "Table creation timeout after 30 seconds"}
    
    async def query_all_data(self, dataset_id, table_id):
        """특정 테이블의 전체 데이터를 조회"""
        try:
            client = await self._get_client()
            query = f"""
            SELECT *
            FROM `{client.project}.{dataset_id}.{table_id}`
            """
            query_job = client.query(query)
            results = query_job.result()
            
            return results

        except Exception as e:
            logger.error(f"Failed to query data: {str(e)}")
            raise Exception(f"BigQuery 데이터 조회 실패: {str(e)}")

    async def query_data_by_date(self, dataset_id, table_id, start_date, end_date):
        """특정 테이블의 특정 날짜 데이터를 조회"""
        try:
            client = await self._get_client()
            query = f"""
            SELECT *
            FROM `{client.project}.{dataset_id}.{table_id}`
            WHERE date BETWEEN DATE('{start_date}') AND DATE('{end_date}')
            """
            query_job = client.query(query)
            results = query_job.result()
            
            return results

        except Exception as e:
            logger.error(f"Failed to query data: {str(e)}")
            raise Exception(f"BigQuery 데이터 조회 실패: {str(e)}")
