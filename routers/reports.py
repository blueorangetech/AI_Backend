from fastapi import APIRouter
from reports import naver
import time, json, os

router = APIRouter(prefix="/reports", tags=["reports"])

@router.post("/naver")
def create_naver_reports():
    master_list = ["Campaign", "Adgroup", "Keyword"]
    stat_list = ["AD"]
    file_list = []

    for master in master_list:
        id = naver.create_master_report(master)

        dounload_url = None
        count = 1
        while dounload_url is None:
            dounload_url = naver.check_status("/master-reports", id)
            count += 1
            time.sleep(0.5)
            if count == 10:
                print("리포트 생성 에러")
                break

        file_name = naver.download_report(dounload_url, master)
        file_list.append(file_name)

        naver.delete_report("/master-reports", id)

    for stat in stat_list:
        id = naver.create_stat_report(stat, "20250121")

        dounload_url = None
        count = 1
        while dounload_url is None:
            dounload_url = naver.check_status("/stat-reports", id)
            count += 1
            time.sleep(0.5)
            if count == 10:
                print("리포트 생성 에러")
                break

        file_name = naver.download_report(dounload_url, stat)
        file_list.append(file_name)
        
        naver.delete_report("/stat-reports", id)
    
    result = naver.merge_reports(file_list)

    for file in file_list:
        if os.path.exists(file):
            os.remove(file)

    return 