from database.mongodb import MongoDB
import httpx
import os, jwt
import logging

logger = logging.getLogger(__name__)


class KakaoTokenManager:
    def __init__(self):
        self.db = MongoDB.get_instance()["Customers"]
        self.client = httpx.AsyncClient()
        self.jwt_token = os.environ["jwt_token_key"]
        self.client_id = os.environ["KAKAO_CLIENT_ID"]
        self.redirect_uri = os.environ["KAKAO_REDIRECT_URL"]

    async def renewal_all_token(self, code):
        url = "https://kauth.kakao.com/oauth/token"

        data = {
            "grant_type": "authorization_code",
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "code": code,
        }

        try:
            response = await self.client.post(url, data=data)
            tokens = response.json()
            access_token, refresh_token = (
                tokens["access_token"],
                tokens["refresh_token"],
            )

            token = {"access_token": access_token, "refresh_token": refresh_token}
            encode_token = jwt.encode(token, self.jwt_token, algorithm="HS256")

            self.db.get_collection("token").update_one(
                {"media": "kakao"}, {"$set": {"token": encode_token}}, upsert=True
            )

            return True

        except httpx.HTTPStatusError as e:
            # HTTP 에러 (400, 401, 500 등)
            raise Exception(f"카카오 API 호출 실패: {e.response.status_code}")

        except Exception as e:
            # 기타 에러
            raise Exception(f"토큰 갱신 실패: {str(e)}")

    async def get_vaild_token(self):
        token_info = await self._get_token_info()
        access_token, refresh_token = (
            token_info["access_token"],
            token_info["refresh_token"],
        )

        check_token_result = await self._check_access_token(access_token)

        if check_token_result is False:
            new_access_token = await self._refresh_access_token(refresh_token)
            return new_access_token

        return access_token

    async def _get_token_info(self):
        try:
            kakao_token = self.db.get_collection("token").find_one({"media": "kakao"})
            if not kakao_token:
                raise Exception("토큰이 데이터베이스에 없습니다.")

            encoded_token = kakao_token.get("token")

            decoded_token = jwt.decode(
                encoded_token, self.jwt_token, algorithms="HS256"
            )
            access_token, refresh_token = decoded_token.get(
                "access_token"
            ), decoded_token.get("refresh_token")

            return {"access_token": access_token, "refresh_token": refresh_token}

        except Exception as e:
            raise Exception(f"DB 에서 토큰 조회 실패: {str(e)}")

    async def _check_access_token(self, access_token):
        try:
            logging.info("토큰 유효성 검사를 시작합니다.")

            url = "https://kapi.kakao.com/v1/user/access_token_info"
            headers = {"Authorization": f"Bearer {access_token}"}
            response = await self.client.get(url, headers=headers)

            if response.status_code == 200:
                logging.info("토큰이 유효합니다")
                return True

            else:
                logging.info("토큰이 유효하지 않습니다.")
                return False

        except:
            logging.info("토큰 검증 작업 오류 발생.")
            return False

    async def _refresh_access_token(self, refresh_token):
        """Refresh 토큰으로 토큰 갱신"""
        try:
            logging.info("Refresh 토큰으로 갱신을 시도합니다.")

            url = "https://kauth.kakao.com/oauth/token"
            headers = {
                "Content-Type": "application/x-www-form-urlencoded;charset=utf-8"
            }
            body = {
                "grant_type": "refresh_token",
                "client_id": self.client_id,
                "refresh_token": refresh_token,
            }

            response = await self.client.post(url, headers=headers, data=body)

            result = response.json()
            if response.status_code == 200:
                new_token = {}
                new_token["access_token"] = result["access_token"]
                new_token["refresh_token"] = (
                    result["refresh_token"]
                    if "refresh_token" in result
                    else refresh_token
                )

                encode_new_token = jwt.encode(
                    new_token, self.jwt_token, algorithm="HS256"
                )

                self.db.get_collection("token").update_one(
                    {"media": "kakao"}, {"$set": {"token": encode_new_token}}
                )
                logging.info("토큰이 갱신되었습니다.")
                return new_token["access_token"]

        except Exception as e:
            logging.info(f"토큰 갱신 오류 발생: {str(e)}")
            return Exception(f"토큰 갱신 오류 발생: {str(e)}")
