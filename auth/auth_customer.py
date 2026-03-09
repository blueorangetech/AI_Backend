from database.mongodb import MongoDB
import bcrypt
import logging

logger = logging.getLogger(__name__)

class LoginAuthManger:
    async def _get_db(self):
        """MongoDB 데이터베이스 연결 반환"""
        mongo_client = await MongoDB.get_instance()
        return mongo_client["Customers"]["members"]

    async def _hash_password(self, password):
        """ 비밀번호 해싱 """
        salt = bcrypt.gensalt()
        hashed = bcrypt.hashpw(password.encode("utf-8"), salt)
        return hashed


    async def _verify_password(self, hashed_password, password):
        """ 비밀번호 검증 """
        return bcrypt.checkpw(password.encode("utf-8"), hashed_password)


    async def store_customer(self, user_id, name, password, access_list=None, is_master=False):
        """ 유저 정보 등록 """
        if access_list is None:
            access_list = {}
            
        hashed = await self._hash_password(password)
        customer_data = {
            "user_id": user_id,
            "name": name, 
            "password": hashed,
            "access_list": access_list,
            "is_master": is_master
        }
        collection = await self._get_db()

        # 이미 존재하는 유저인지 확인 (ID 기준)
        if collection.find_one({"user_id": user_id}):
            return False

        collection.insert_one(customer_data)
        logger.info(f"New Customer {name}({user_id}) Created (Master: {is_master})")
        return True


    async def authenticate(self, user_id, password):
        """ 유저 인증 (ID 기준) """
        collection = await self._get_db()
        customer = collection.find_one({"user_id": user_id})
        
        error_msg = "아이디 혹은 비밀번호가 일치하지 않습니다."

        if customer is None:
            return {"status": False, "message": error_msg}
            
        stored_password = customer["password"]
        if await self._verify_password(stored_password, password):
            return {"status": True, "message": "인증 성공", "customer": customer}
            
        else:
            return {"status": False, "message": error_msg}

    async def check_authority(self, customer_url, access_list, is_master=False):
        """ 유저 권한 검증 """
        if is_master:
            return {"status": True, "role": "master"}
            
        role = access_list.get(customer_url)
        
        if role:
            return {"status": True, "role": role}
        
        return {"status": False, "message": "해당 페이지에 대한 접근 권한이 없습니다."}

    async def update_customer_privilege(self, user_id, access_list=None, is_master=None):
        """ 유저 권한 업데이트 (ID 기준) """
        collection = await self._get_db()
        update_data = {}
        
        if access_list is not None:
            update_data["access_list"] = access_list
        if is_master is not None:
            update_data["is_master"] = is_master
            
        if not update_data:
            return False
            
        result = collection.update_one({"user_id": user_id}, {"$set": update_data})
        return result.modified_count > 0

    async def get_user_by_id(self, user_id):
        """ ID로 단일 유저 조회 """
        collection = await self._get_db()
        return collection.find_one({"user_id": user_id}, {"password": 0, "_id": 0})

    async def get_all_customers(self):
        """ 모든 유저 정보 조회 (비밀번호 제외) """
        collection = await self._get_db()
        customers = list(collection.find({}, {"password": 0, "_id": 0}))
        return customers