from clients.ga4_api_client import GA4APIClient

class GA4ReportServices:

    def __init__(self, google_client: GA4APIClient):
        self.client = google_client
    

    def check_events(self):
        self.client.check_events()

        return
    
    def properties_list(self):
        self.client.properties_list()
        return