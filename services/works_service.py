from clients.works_api_client import WorksAPIClient
import pandas as pd
import asyncio, logging, io, base64

logger = logging.getLogger(__name__)

class WorksService:
    def __init__(self, access_token: str):
        self.client = WorksAPIClient(access_token)

    async def send_mail(self, content):
        response = await self.client.send_mail(content)
        return response

    async def read_mails(self, folder, report_name, field_names):
        response = await self.client.read_folder(folder)
        mails = response["detail"]["mails"]
        
        results = {"data": [], "mail_ids": []}
        for mail in mails:
            subject = mail["subject"]

            if report_name in subject:
                mail_id = mail["mailId"]
                content = await self.client.read_mail(mail_id)
                attachmentId = content["detail"]["attachments"][0]["attachmentId"]
                
                data = await self.client.download_attachment(mail_id, attachmentId)
                byte_data = base64.b64decode(data["detail"]["data"])
                df = pd.read_excel(io.BytesIO(byte_data), engine='openpyxl')
                data = df.iloc[:-1]
                data.columns = field_names

                result = data.to_dict("records")

                results["data"].extend(result)
                results["mail_ids"].append(mail_id)

        return results
    
    async def delete_mails(self, mail_ids):
        for mail_id in mail_ids:
            await self.client.delete_mail(mail_id)

