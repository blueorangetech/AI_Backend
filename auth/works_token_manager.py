from database.mongodb import MongoDB
from utils.http_client_manager import get_http_client
from datetime import datetime
import os, jwt, logging

logger = logging.getLogger(__name__)

class WorksTokenManager:
    def __init__(self):
        self.jwt_token = os.environ["jwt_token_key"]
        self.client_id = os.environ["WORKS_CLIENT_ID"]
        self.client_secret = os.environ["WORKS_CLIENT_SECRET"]
    
    async def _get_db(self):
        """MongoDB 데이터베이스 연결 반환"""
        mongo_client = await MongoDB.get_instance()
        return mongo_client["Customers"]
    
    async def _get_token_info(self):
        db = await self._get_db()
        gfa_token = db.get_collection("token").find_one({"media": "works"})
        if not gfa_token:
            raise Exception("토큰이 데이터베이스에 없습니다.")
        
        encoded_token = gfa_token.get("token")

        decoded_token = jwt.decode(
                encoded_token, self.jwt_token, algorithms="HS256"
            )
        
        access_token = decoded_token.get("access_token")
        refresh_token = decoded_token.get("refresh_token")
        
        return {"access_token": access_token, "refresh_token": refresh_token}
    
    async def _validate_token(self, access_token):
        """토큰 유효성 검사"""
        try:
            url = "https://www.worksapis.com/v1.0/users/me/mail/unread-count"
            headers = {"Authorization": f"Bearer {access_token}"}
            client = await get_http_client()
            response = await client.get(url, headers=headers)

            if response.status_code == 200:
                logging.info("토큰이 유효합니다")
                return True

            else:
                logging.info("토큰이 유효하지 않습니다.")
                return False
        
        except Exception as e:
            logger.error(f"토큰 유효성 검사 실패: {str(e)}")
            return False
        
    async def _refresh_access_token(self, refresh_token):
        """Access Token 갱신"""
        client = await get_http_client()

        refresh_url = "https://auth.worksmobile.com/oauth2/v2.0/token"
        params = {
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }

        try:
            response = await client.post(refresh_url, params=params)
            response.raise_for_status()
            token_data = response.json()

            new_access_token = token_data["access_token"]

            token = {"access_token": new_access_token, "refresh_token": refresh_token}
            encode_token = jwt.encode(token, self.jwt_token, algorithm="HS256")

            db = await self._get_db()
            db.get_collection("token").update_one(
                {"media": "works"}, {"$set": {"token": encode_token}
                }, upsert=True
            )
            logger.info("Access Token이 성공적으로 갱신되었습니다")

            return new_access_token
        
        except Exception as e:
            logger.error(f"토큰 갱신 실패: {str(e)}")
            raise

    async def get_vaild_token(self):
        token_info = await self._get_token_info()
        access_token, refresh_token = (
            token_info["access_token"],
            token_info["refresh_token"],
        )
        
        check_token_result = await self._validate_token(access_token)
        
        if check_token_result is False:
            new_access_token = await self._refresh_access_token(refresh_token)
            return new_access_token
        
        return access_token
    
    async def renewal_all_token(self, code):
        token_url = "https://auth.worksmobile.com/oauth2/v2.0/token"

        token_data = {
            "code": code,
            "grant_type": "authorization_code",
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }

        try:
            client = await get_http_client()
            response = await client.post(token_url, data=token_data)
            token_reponse = response.json()

            if "access_token" in token_reponse:
                access_token = token_reponse["access_token"]
                refresh_token = token_reponse.get("refresh_token", "")

                token = {"access_token": access_token, "refresh_token": refresh_token}
                encode_token = jwt.encode(token, self.jwt_token, algorithm="HS256")

                db = await self._get_db()
                db.get_collection("token").update_one(
                    {"media": "works"}, {"$set": {
                        "token": encode_token,
                        "updated_at": datetime.now()
                        }
                    }, 
                    upsert=True
                )

                logger.info(f"\n Works 토큰 갱신 완료!")
                return True
            
            else:
                logger.info(f"Works 토큰 갱신 실패: {token_reponse}")
                return False

        except Exception as e:
            logger.error(f"Works 토큰 갱신 실패: {e}")
            return False