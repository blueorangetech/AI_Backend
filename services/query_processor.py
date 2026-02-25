from typing import Optional

# 고객사별 + 리포트 타입별 설정
class BoCustomerQuery:
    CONFIGS = {
        "hanssem": {
            "trend": {
                "metrics": [
                    "SUM(impressions) AS impressions", "SUM(clicks) AS clicks",
                    "SUM(cost) AS cost", "SUM(consultation) AS consultation",
                    "SUM(distribution) AS distribution",
                    "SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100 AS ctr",
                    "SAFE_DIVIDE(SUM(cost), SUM(clicks)) AS cpc",
                    "SAFE_DIVIDE(SUM(distribution), SUM(consultation)) * 100 AS cvr",
                    "SAFE_DIVIDE(SUM(cost), SUM(distribution)) AS cpa",
                    ],
                "group_by": ["date"],
                "order_by": "date ASC"
            },
            "material": {
                "metrics": [
                    "SUM(impressions) AS impressions", "SUM(clicks) AS clicks",
                    "SUM(cost) AS cost", "SUM(consultation) AS consultation",
                    "SUM(distribution) AS distribution",
                    "SAFE_DIVIDE(SUM(clicks), SUM(impressions)) * 100 AS ctr",
                    "SAFE_DIVIDE(SUM(cost), SUM(clicks)) AS cpc",
                    "SAFE_DIVIDE(SUM(distribution),SUM(consultation)) * 100 AS cvr",
                    "SAFE_DIVIDE(SUM(cost), SUM(distribution)) AS cpa"
                    ],

                "group_by": ["media", "utm_content", "utm_content_1", "utm_content_2",
                            "utm_content_3", "utm_content_4", "utm_content_5", 
                            "utm_content_6", "utm_content_7"
                            ],
                "order_by": "CASE WHEN cpa = 0 OR cpa IS NULL THEN 1 ELSE 0 END ASC, cpa ASC"
            }
        }
    }
    @classmethod
    def get_query(cls, dataset_id: str, table_id: str, 
                  report_type: str, start_date: str, end_date: str,
                  limit: Optional[int]  = None, offset: int = 0,
                  min_cost: Optional[float] = None, min_distribution: Optional[float] = None):
        
        # 1. 고객사 및 리포트 타입 설정 로드
        customer_cfg = cls.CONFIGS.get(dataset_id)
        if not customer_cfg: raise ValueError("Client not found")
        
        report_cfg = customer_cfg.get(report_type)
        if not report_cfg: raise ValueError("Report type not found")
        
        having_clauses = []
        if min_cost is not None:
            having_clauses.append(f"cost >= {min_cost}")

        if min_distribution is not None:
            # cpa는 이미 별칭이므로 HAVING 절에서 직접 계산식을 쓰거나 별칭을 지원하는 경우 사용
            having_clauses.append(f"distribution >= {min_distribution}")

        having_str = f"HAVING {' AND '.join(having_clauses)}" if having_clauses else ""

        # 2. 쿼리 조립
        query = f"""
            SELECT 
                {", ".join(report_cfg["group_by"])},
                {", ".join(report_cfg["metrics"])}
            FROM `{{project_id}}.{dataset_id}.{table_id}`
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY {", ".join(report_cfg["group_by"])}
            {having_str}
            ORDER BY {report_cfg["order_by"]}
        """

        if limit is not None:
            query += f" LIMIT {limit} OFFSET {offset}"

        return query
        
    