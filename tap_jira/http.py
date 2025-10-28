from datetime import datetime, timedelta
import time
import threading
import re
import json 
from requests.exceptions import (HTTPError, Timeout)
from requests.auth import HTTPBasicAuth
import requests
from singer import metrics
import singer
import backoff

# Jira OAuth tokens last for 3600 seconds. We set it to 3500 to try to
# come in under the limit.
REFRESH_TOKEN_EXPIRATION_PERIOD = 3500

# The project plan for this tap specified:
# > our past experience has shown that issuing queries no more than once every
# > 10ms can help avoid performance issues
TIME_BETWEEN_REQUESTS = timedelta(microseconds=10e3)

LOGGER = singer.get_logger()

# timeout requests after 300 seconds
REQUEST_TIMEOUT = 300

class JiraError(Exception):
    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response

class JiraBackoffError(JiraError):
    pass

class JiraBadRequestError(JiraError):
    pass

class JiraUnauthorizedError(JiraError):
    pass

class JiraForbiddenError(JiraError):
    pass

class JiraSubRequestFailedError(JiraError):
    pass

class JiraBadGatewayError(JiraError):
    pass

class JiraConflictError(JiraError):
    pass

class JiraNotFoundError(JiraError):
    pass

class JiraRateLimitError(JiraBackoffError):
    pass

class JiraServiceUnavailableError(JiraBackoffError):
    pass

class JiraGatewayTimeoutError(JiraError):
    pass

class JiraInternalServerError(JiraError):
    pass

class JiraNotImplementedError(JiraError):
    pass

def should_retry_httperror(exception):
    """ Retry 500-range errors. """
    # An ConnectionError is thrown without a response
    if exception.response is None:
        return True

    return 500 <= exception.response.status_code < 600

ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": JiraBadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": JiraUnauthorizedError,
        "message": "Invalid authorization credentials."
    },
    403: {
        "raise_exception": JiraForbiddenError,
        "message": "User does not have permission to access the resource."
    },
    404: {
        "raise_exception": JiraNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    409: {
        "raise_exception": JiraConflictError,
        "message": "The request does not match our state in some way."
    },
    429: {
        "raise_exception": JiraRateLimitError,
        "message": "The API rate limit for your organisation/application pairing has been exceeded."
    },
    449:{
        "raise_exception": JiraSubRequestFailedError,
        "message": "The API was unable to process every part of the request."
    },
    500: {
        "raise_exception": JiraInternalServerError,
        "message": "The server encountered an unexpected condition which prevented" \
            " it from fulfilling the request."
    },
    501: {
        "raise_exception": JiraNotImplementedError,
        "message": "The server does not support the functionality required to fulfill the request."
    },
    502: {
        "raise_exception": JiraBadGatewayError,
        "message": "Server received an invalid response."
    },
    503: {
        "raise_exception": JiraServiceUnavailableError,
        "message": "API service is currently unavailable."
    },
    504: {
        "raise_exception": JiraGatewayTimeoutError,
        "message": "API service time out, please check Jira server."
    }
}

def check_status(response):
    # Forming a response message for raising custom exception
    try:
        response_json = response.json()
    except Exception: # pylint: disable=broad-except
        response_json = {}
    if response.status_code != 200:
        message = "HTTP-error-code: {}, Error: {}".format(
            response.status_code,
            response_json.get("errorMessages", [ERROR_CODE_EXCEPTION_MAPPING.get(
                response.status_code, {}).get("message", "Unknown Error")])[0]
        )
        exc = ERROR_CODE_EXCEPTION_MAPPING.get(
            response.status_code, {}).get("raise_exception", JiraError)
        raise exc(message, response) from None

def get_request_timeout(config):
    # Get `request_timeout` value from config
    config_request_timeout = config.get('request_timeout')

    # if config request_timeout is other than 0, "0", or "" then use request_timeout
    if config_request_timeout and float(config_request_timeout):
        request_timeout = float(config_request_timeout)
    else:
        # If value is 0, "0", "", or not passed then it set default to 300 seconds
        request_timeout = REQUEST_TIMEOUT
    return request_timeout

class Client():
    def __init__(self, config):
        self.is_cloud = 'oauth_client_id' in config.keys()
        self.session = requests.Session()
        self.next_request_at = datetime.now()
        self.user_agent = config.get("user_agent")
        self.login_timer = None
        self.timeout = get_request_timeout(config)
        self.is_on_prem_instance = False

        if self.is_cloud:
            # This logic remains for anyone using OAuth, which is not you.
            # LOGGER.info("Using OAuth based API authentication")
            self.auth_method = 'oauth'
            self.base_url = 'https://api.atlassian.com/ex/jira/{}{}'
            self.cloud_id = config.get('cloud_id')
            self.access_token = config.get('access_token')
            self.refresh_token = config.get('refresh_token')
            self.oauth_client_id = config.get('oauth_client_id')
            self.oauth_client_secret = config.get('oauth_client_secret')
            self.refresh_credentials()
            self.test_credentials_are_authorized()
        
        # --- MODIFIED ---
        # This is the new logic for your pre-encoded Basic Auth key.
        elif 'api_key' in config:
            # LOGGER.info("Using Pre-encoded Basic Auth API Key")
            self.auth_method = 'api_key'
            self.base_url = config.get("base_url")
            self.api_key = config.get("api_key")
            if not self.api_key:
                raise Exception("Configuration is missing required key: 'api_key'")
            self.test_basic_credentials_are_authorized()
        # --- END MODIFIED ---
            
        else:
            # This is the original legacy Basic Auth logic.
            # LOGGER.info("Using Basic Auth API authentication")
            self.auth_method = 'basic'
            self.base_url = config.get("base_url")
            self.auth = HTTPBasicAuth(config.get("username"), config.get("password"))
            self.test_basic_credentials_are_authorized()

    def url(self, path):
        if not path:
            # Prevent NoneType error and log diagnostic info
            # LOGGER.warning("[DEBUG] Client.url() called with None path — returning base_url only")
            return self.base_url

        if self.is_cloud:
            return self.base_url.format(self.cloud_id, path)

        # defend against if the base_url does or does not provide https://
        base_url = self.base_url
        base_url = re.sub('^http[s]?://', '', base_url)
        base_url = 'https://' + base_url
        return base_url.rstrip("/") + "/" + path.lstrip("/")


    def _headers(self, headers):
        headers = headers.copy()
        if self.user_agent:
            headers["User-Agent"] = self.user_agent

        if self.is_cloud:
            headers['Accept'] = 'application/json'
            headers['Authorization'] = 'Bearer {}'.format(self.access_token)
            
        # --- MODIFIED ---
        # If we are using the api_key method, set the header directly.
        elif self.auth_method == 'api_key':
            headers['Accept'] = 'application/json'
            headers['Authorization'] = f"Basic {self.api_key}"
        # --- END MODIFIED ---

        return headers

    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.ConnectionError, HTTPError, Timeout),
                          jitter=None,
                          max_tries=6,
                          giveup=lambda e: not should_retry_httperror(e))
    def send(self, method, path, headers={}, **kwargs):
        # --- MODIFIED ---
        # This logic is simplified. We build the full headers first, then decide
        # whether to pass the `auth` object for legacy basic auth.
        full_headers = self._headers(headers)
        
        # The `auth` object is only needed for the original username/password method.
        # For OAuth and our new api_key method, the Authorization header is already set.
        auth_object = self.auth if self.auth_method == 'basic' else None

        request = requests.Request(method,
                                   self.url(path),
                                   auth=auth_object,
                                   headers=full_headers,
                                   **kwargs)
        # --- END MODIFIED ---
        return self.session.send(request.prepare(), timeout=self.timeout)

    @backoff.on_exception(backoff.constant,
                          JiraBackoffError,
                          max_tries=10,
                          interval=60)
    def request(self, tap_stream_id, method, path, **kwargs):
        wait = (self.next_request_at - datetime.now()).total_seconds()
        if wait > 0:
            time.sleep(wait)
        # --- ADD THIS DEBUGGING BLOCK ---
        if 'json' in kwargs:
            LOGGER.info(f"JIRA REQUEST PAYLOAD for '{path}': {json.dumps(kwargs['json'])}")
        # --- END DEBUGGING BLOCK ---    
        with metrics.http_request_timer(tap_stream_id) as timer:
            response = self.send(method, path, **kwargs)
            self.next_request_at = datetime.now() + TIME_BETWEEN_REQUESTS
            timer.tags[metrics.Tag.http_status_code] = response.status_code
        check_status(response)
        try:
            return response.json()
        except ValueError:
            return response.text

        # --- NEW METHOD FOR FETCHING INDIVIDUAL ISSUES ---
    @backoff.on_exception(backoff.constant,
                      JiraBackoffError,
                      max_tries=10,
                      interval=60)
    def fetch_issue_details(self, issue_id_or_key, tap_stream_id="issues", fields="*all", expand="changelog,transitions"):
        path = f"/rest/api/3/issue/{issue_id_or_key}"
        params = {
            "fields": fields,
            "expand": expand
        }
        # LOGGER.info(f"[DEBUG] Fetching detailed issue {issue_id_or_key} with fields={fields}, expand={expand}")
        # Use a GET request for individual issue details
        return self.request(tap_stream_id, "GET", path, params=params)

    # backoff for Timeout error is already included in "Exception"
    # as it's a parent class of "Timeout" error
    @backoff.on_exception(backoff.expo, Exception, max_tries=3)
    def refresh_credentials(self):
        body = {"grant_type": "refresh_token",
                "client_id": self.oauth_client_id,
                "client_secret": self.oauth_client_secret,
                "refresh_token": self.refresh_token}
        resp = None
        try:
            resp = self.session.post(
                "https://auth.atlassian.com/oauth/token",
                data=body,
                timeout=self.timeout)
            resp.raise_for_status()
            self.access_token = resp.json()['access_token']
        except Exception as ex:
            error_message = str(ex)
            if resp:
                error_message = error_message + ", Response from Jira: {}".format(resp.text)
            raise Exception(error_message) from ex
        finally:
            # LOGGER.info("Starting new login timer")
            self.login_timer = threading.Timer(REFRESH_TOKEN_EXPIRATION_PERIOD,
                                               self.refresh_credentials)
            self.login_timer.start()

    def test_credentials_are_authorized(self):
        # Assume that everyone has issues, so we try and hit that endpoint
        self.request("issues", "POST", "/rest/api/3/search/jql", # This path is incorrect for GET, but client.request will convert it to POST if json body is provided.
                     json={"maxResults": 1, "jql": "ORDER BY created DESC"}) # ✅ Use POST with JSON body for /search/jql

    def test_basic_credentials_are_authorized(self):
        # Make a call to myself endpoint for verify creds
        # Here, we are retrieving serverInfo for the Jira instance by which credentials will also be verified.
        # Assign True value to is_on_prem_instance property for on-prem Jira instance
        self.is_on_prem_instance = self.request("users","GET","/rest/api/3/serverInfo").get('deploymentType') == "Server"

# In your http.py file

class Paginator():
    def __init__(self, client, items_key="values"):
        self.client = client
        self.items_key = items_key
        # Start with a sentinel value to run the loop once.
        self.next_page_token = ""

    def pages(self, *args, **kwargs):
        """
        Returns a generator which yields pages of data using token-based pagination.
        This method expects to receive a 'json' dictionary in kwargs for the initial request.
        """
        # Make a copy of the initial request body.
        json_body = kwargs.pop("json", {}).copy()

        while self.next_page_token is not None:
            # For subsequent requests, add the token to the existing payload.
            # DO NOT create a new payload. The API needs the original JQL context.
            if self.next_page_token: # Only add it if it's not the empty-string first run
                json_body["nextPageToken"] = self.next_page_token
            
            # For the first run, ensure no old token is present
            elif "nextPageToken" in json_body:
                del json_body["nextPageToken"]

            response = self.client.request(*args, json=json_body, **kwargs)

            page = response.get(self.items_key, [])
            
            is_last = response.get("isLast", True)
            
            # Get the token for the next iteration.
            self.next_page_token = response.get("nextPageToken") if not is_last else None

            if page:
                yield page
