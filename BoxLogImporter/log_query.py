import requests
import datetime
import logging

class LogQuery:
    def __init__(self, client_id: str, client_secret: str, tenant_id: str, workspace_id: str):
        self.login_url = "https://login.microsoftonline.com/{tenant_id}/oauth2/token/".format(tenant_id=tenant_id)
        self.resource = "https://api.loganalytics.io"
        self.base_url = "https://api.loganalytics.azure.com"
        self.query_url = "{base_url}/v1/workspaces/{workspace_id}/query".format(base_url=self.base_url, workspace_id=workspace_id)
        self.client_id = client_id
        self.client_secret = client_secret
        self.expire_on = 0
        self.access_token = None

    def query(self, query: str):
        self._update_auth_token()

        # https://learn.microsoft.com/en-us/rest/api/loganalytics/dataaccess/query/execute?tabs=HTTP
        headers = {
            'Authorization': self.access_token
        }
        body = {
            'query': query
        }

        try:
            response = requests.post(self.query_url, json=body, headers=headers)
        except Exception as err:
            logging.error("Error during executing an Analytics query: {}".format(err))
            return None
        else:
            if (response.status_code == 200):
                json = response.json()
                logging.info('{}: {}'.format(response.status_code, json))
                return json['tables'][0]['rows']
            else:
                logging.error("Error during executing an Analytics query. Error code: {}".format(response.status_code))
                return None

    def _update_auth_token(self):
        current_time = int(datetime.datetime.utcnow().timestamp())
        if self.expire_on > current_time:
            logging.info("No need to update auth token.")
            return
        
        # https://learn.microsoft.com/en-us/azure/azure-monitor/logs/api/access-api#client-credentials-flow
        headers = {
            'content-type': 'application/x-www-form-urlencoded'
        }
        body = {
            'grant_type': 'client_credentials',
            'resource': self.resource,
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }

        try:
            response = requests.post(self.login_url, data=body, headers=headers)
        except Exception as err:
            logging.error("Error during get authentication token: {}".format(err))
        else:
            if (response.status_code >= 200 and response.status_code <= 299):
                r = response.json()
                #logging.info('Status Code: {}'.format(response.status_code))
                #logging.info('expires_in: {}'.format(r['expires_in']))
                #logging.info('expires_on: {}'.format(r['expires_on']))
                #ogging.info('access_token: {}'.format(r['access_token']))
                self.expire_on = int(r['expires_in']) + int(datetime.datetime.utcnow().timestamp())
                self.access_token = '{} {}'.format(r['token_type'], r['access_token'])
                #logging.info('{}: {}'.format(response.status_code, response.json()))
            else:
                logging.error("Authentication failed. Error code: {}".format(response.status_code))
                