from fastapi import APIRouter, HTTPException
from bson import json_util

from database.mongodb import MongoDB

import traceback, json

router = APIRouter(prefix="/mmm", tags=["mix_models"])
mongodb = MongoDB.get_instance()
db = mongodb["MixModel"]

@router.get("/{customer_url}")
async def get_mmm_list(customer_url: str):
    mmm_db = db[customer_url]
    try:
        mmm_list = mmm_db.find()
        result = []
        for m in mmm_list:
            each_model = {"id": m["_id"], "rsq": m["rsq"], "mape": m["mape"]}
            result.append(each_model)

        return json.dumps(result, default=json_util.default)
    
    except:
        traceback.print_exc()
        raise HTTPException(status_code=404, detail="시스템 에러 발생")