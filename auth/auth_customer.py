from database.mongodb import MongoDB
import bcrypt
import logging

logger = logging.getLogger(__name__)

class LoginAuthManger:
    async def _get_db(self):
        """MongoDB 데이터베이스 연결 반환"""
        mongo_client = await MongoDB.get_instance()
        return mongo_client["Customers"]["info"]

    async def _hash_password(self, password):
        """ 비밀번호 해싱 """
        salt = bcrypt.gensalt()
        hashed = bcrypt.hashpw(password.encode("utf-8"), salt)
        return hashed


    async def _verify_password(self, hashed_password, password):
        """ 비밀번호 검증 """
        return bcrypt.checkpw(password.encode("utf-8"), hashed_password)


    async def store_customer(self, name, password):
        """ 유저 정보 등록 """
        hashed = await self._hash_password(password)
        customer_data = {"name": name, "password": hashed}
        collection = await self._get_db()

        collection.insert_one(customer_data)
        logger.info(f"New Customer{name} Created !")


    async def authenticate(self, name, password):
        """ 유저 인증 """
        collection = await self._get_db()
        customer = collection.find_one({"name": name})
        
        if customer is not None:
            stored_password = customer["password"]

            return await self._verify_password(stored_password, password)

        else:
            logger.info(f"Customer {name} is not existed, Create {name}...")
            await self.store_customer(name, password)
            return True