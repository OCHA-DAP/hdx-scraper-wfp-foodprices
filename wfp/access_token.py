class AccessToken:
    def __init__(self, configuration, token_downloader, downloader):
        self.configuration = configuration
        self.token_downloader = token_downloader
        self.downloader = downloader

    def refresh(self):
        self.token_downloader.download(
            self.configuration["token_url"],
            post=True,
            parameters={"grant_type": "client_credentials"},
        )
        bearer_token = self.token_downloader.get_json()["access_token"]
        self.downloader.set_bearer_token(bearer_token)
