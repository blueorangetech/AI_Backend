from clients.google_ads_api_client import GoogleAdsAPIClient

class GoogleReportServices:

    def __init__(self, google_client: GoogleAdsAPIClient):
        self.client = google_client
    

    def create_keyword_reports(self):
        self.client.create_report()
        return