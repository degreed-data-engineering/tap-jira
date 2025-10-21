from datetime import datetime, timedelta
import time
import threading
import re
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

        # Assign False for cloud Jira instance
        self.is_on_prem_instance = False

        if self.is_cloud:
            LOGGER.info("Using OAuth based API authentication")
            self.auth = None
            self.base_url = 'https://api.atlassian.com/ex/jira/{}{}'
            self.cloud_id = config.get('cloud_id')
            self.access_token = config.get('access_token')
            self.refresh_token = config.get('refresh_token')
            self.oauth_client_id = config.get('oauth_client_id')
            self.oauth_client_secret = config.get('oauth_client_secret')

            # Only appears to be needed once for any 6 hour period. If
            # running the tap for more than 6 hours is needed this will
            # likely need to be more complicated.
            self.refresh_credentials()
            self.test_credentials_are_authorized()
        else:
            LOGGER.info("Using Basic Auth API authentication")
            self.base_url = config.get("base_url")
            self.auth = HTTPBasicAuth(config.get("username"), config.get("password"))
            self.test_basic_credentials_are_authorized()

    def url(self, path):
        if not path:
            # Prevent NoneType error and log diagnostic info
            LOGGER.warning("[DEBUG] Client.url() called with None path — returning base_url only")
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
            # Add OAuth Headers
            headers['Accept'] = 'application/json'
            headers['Authorization'] = 'Bearer {}'.format(self.access_token)

        return headers

    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.ConnectionError, HTTPError, Timeout),
                          jitter=None,
                          max_tries=6,
                          giveup=lambda e: not should_retry_httperror(e))
    def send(self, method, path, headers={}, **kwargs):
        if self.is_cloud:
            # OAuth Path
            request = requests.Request(method,
                                       self.url(path),
                                       headers=self._headers(headers),
                                       **kwargs)
        else:
            # Basic Auth Path
            request = requests.Request(method,
                                       self.url(path),
                                       auth=self.auth,
                                       headers=self._headers(headers),
                                       **kwargs)
        return self.session.send(request.prepare(), timeout=self.timeout)

    @backoff.on_exception(backoff.constant,
                      JiraBackoffError,
                      max_tries=10,
                      interval=60)
    def request(self, tap_stream_id, method, path, **kwargs):
        wait = (self.next_request_at - datetime.now()).total_seconds()
        if wait > 0:
            time.sleep(wait)

        with metrics.http_request_timer(tap_stream_id) as timer:
            response = self.send(method, path, **kwargs)
            self.next_request_at = datetime.now() + TIME_BETWEEN_REQUESTS
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        # 🧭 DEBUG LOGGING BLOCK START
        params = kwargs.get("params", {}) or kwargs.get("json", {})
        LOGGER.warning(
            f"[DEBUG PAGINATION] stream={tap_stream_id} | path={path} | "
            f"startAt={params.get('startAt')} | maxResults={params.get('maxResults')} | "
            f"status={response.status_code}"
        )
        # 🧭 DEBUG LOGGING BLOCK END

        check_status(response)
        try:
            return response.json()
        except ValueError:
            return response.text



    # backoff for Timeout error is already included in "Exception"
    # as it's a parent class of "Timeout" error
    @backoff.on_exception(backoff.expo, Exception, max_tries=3)
    def refresh_credentials(self):
        body = {"grant_type": "refresh_token",
                "client_id": self.oauth_client_id,
                "client_secret": self.oauth_client_secret,
                "refresh_token": self.refresh_token}
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
            LOGGER.info("Starting new login timer")
            self.login_timer = threading.Timer(REFRESH_TOKEN_EXPIRATION_PERIOD,
                                               self.refresh_credentials)
            self.login_timer.start()

    def test_credentials_are_authorized(self):
        # Assume that everyone has issues, so we try and hit that endpoint
        self.request("issues", "GET", "/rest/api/3/search/jql",
                     params={"maxResults": 1})

    def test_basic_credentials_are_authorized(self):
        # Make a call to myself endpoint for verify creds
        # Here, we are retrieving serverInfo for the Jira instance by which credentials will also be verified.
        # Assign True value to is_on_prem_instance property for on-prem Jira instance
        self.is_on_prem_instance = self.request("users","GET","/rest/api/3/serverInfo").get('deploymentType') == "Server"

class Paginator():
    def __init__(self, client, page_num=0, order_by=None, items_key="values"):
        self.client = client
        self.next_page_num = page_num
        self.order_by = order_by
        self.items_key = items_key

    def pages(self, *args, **kwargs):
        """Returns a generator which yields pages of data."""

        params = kwargs.pop("params", {}).copy()
        while self.next_page_num is not None:
            # Always ensure defaults
            params.setdefault("maxResults", 50)
            params["startAt"] = self.next_page_num
            if self.order_by:
                params["orderBy"] = self.order_by

            # ✅ Add log line to trace pagination
            LOGGER.info(f"Fetching Jira page: startAt={params['startAt']}, maxResults={params['maxResults']}")

            response = self.client.request(*args, params=params, **kwargs)

            if self.items_key:
                page = response.get(self.items_key, [])
            else:
                page = response

            max_results = response.get("maxResults", params["maxResults"])
            total = response.get("total", "unknown")
            LOGGER.info(f"Got {len(page)} records (of total={total}) from Jira response")

            if len(page) < max_results:
                LOGGER.info("No more pages remaining — stopping pagination.")
                self.next_page_num = None
            else:
                self.next_page_num += max_results
                LOGGER.info(f"Next page will startAt={self.next_page_num}")

            if page:
                LOGGER.info(f"Yielded page with {len(page)} records; continuing...")
                yield page



class IssuesPaginator(Paginator):
    """Custom paginator for Jira Cloud v3 `/search/jql` endpoint.

    Uses POST with JSON body instead of GET query params.
    Adds verification log for 51st record to confirm correct pagination.
    """

    def pages(self, *args, **kwargs):
        params = kwargs.pop("json", {}).copy()
        has_more_pages = True

        start_at = params.get("startAt", 0)
        max_results = params.get("maxResults", 50)

        first_page_first_issue_id = None
        total_pages = 0

        while has_more_pages:
            total_pages += 1
            body = {
                "jql": params.get("jql"),
                "fields": params.get("fields", "*all"),
                "expand": params.get("expand", "changelog,transitions"),
                "validateQuery": params.get("validateQuery", "strict"),
                "startAt": start_at,
                "maxResults": max_results,
            }

            LOGGER.info(
                f"[DEBUG PAGINATION] 🔄 Sending POST /rest/api/3/search/jql "
                f"with startAt={start_at}, maxResults={max_results}"
            )

            response = self.client.request("POST", "/rest/api/3/search/jql", json=body)

            if not response:
                LOGGER.warning("[DEBUG PAGINATION] ⚠️ Empty response; stopping pagination.")
                break

            page = response.get(self.items_key) if self.items_key else response

            page_size = len(page or [])
            LOGGER.info(
                f"[DEBUG PAGINATION] 📄 Page {total_pages} → startAt={response.get('startAt')} "
                f"isLast={response.get('isLast')} total={response.get('total', 'unknown')} page_size={page_size}"
            )

            if not page:
                LOGGER.info("[DEBUG PAGINATION] ❌ No issues in this page; stopping.")
                break

            # 🧩 Capture first issue ID from first page for comparison
            if total_pages == 1 and page_size > 0:
                first_page_first_issue_id = page[0].get("id")
                LOGGER.info(f"[DEBUG PAGINATION] 🆕 First issue on page 1: ID={first_page_first_issue_id}")

            # 🧩 Capture first issue ID on page 2 (the 51st issue overall)
            if total_pages == 2 and page_size > 0:
                second_page_first_issue_id = page[0].get("id")
                LOGGER.info(
                    f"[DEBUG PAGINATION] 🧩 51st record check → First issue on page 2: "
                    f"ID={second_page_first_issue_id}"
                )
                if second_page_first_issue_id == first_page_first_issue_id:
                    LOGGER.warning(
                        "⚠️ Pagination check FAILED — 51st record is identical to 1st record! "
                        "Likely using GET-style pagination or missing startAt handling."
                    )
                else:
                    LOGGER.info("✅ Pagination check PASSED — 51st record is different, pagination working correctly.")

            # Determine if there are more pages
            has_more_pages = not response.get("isLast", False)
            start_at = response.get("startAt", start_at) + page_size

            yield page




