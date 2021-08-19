from requests import Request, Session
from requests.exceptions import HTTPError
import json
class EsApiCall:
    def __init__(self, client, method: str = None):
        self._client = client
        self._method = method

    def __getattr__(self, method: str):
        method = method.upper()
        return EsApiCall(self._client, method)

    def __call__(self, *args, **kwargs):
        return self._client.api_call(self._method, kwargs)


class EsClient:
    def __init__(self, log, es_url: str, user: str = None, password: str = None):
        self._es_url = es_url
        self._chek_connection()
        self._session = Session()
        self.log = log
        self._user = user
        self._password = password

        if self._user:
            log.debug("set user pass {}/***".format(self._user))
            self._session.auth = (self._user, self._password)

    def _chek_connection(self):
        pass

    def api_call(self, method, param):
        self.log.debug("Method: {}, param: {}".format(method, param))
        headers = {}
        if '?' in param['path']:
            path = param['path']
        else:
            path = param['path'] + "?format=json&pretty"

        _request = Request(method, self._es_url + path, headers=headers)
        if 'data' in param:
            headers['content-type'] = 'application/json'
            _request.data = json.dumps(param['data'])

        prepped = _request.prepare()
        resp = self._session.send(prepped, verify=False)

        if method not in ['HEAD']:
            response = resp.json()
        else:
            response = {}

        response_code = resp.status_code
        self.log.debug("response code {}, {} ".format(resp.status_code, response))
        return response, int(response_code)

    def get_api(self):
        return EsApiCall(self)
