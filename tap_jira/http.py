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
            # ✅ For POST with JSON body, use the body as-is (no prefixing!)
            if method == "POST" and ("json" in kwargs or "body" in kwargs):
                # Normalize for backward compatibility
                if "body" in kwargs and "json" not in kwargs:
                    kwargs["json"] = kwargs.pop("body")

            response = self.send(method, path, **kwargs)
            if "json" in kwargs:
                try:
                    LOGGER.warning(f"[DEBUG CLIENT.REQUEST] 📦 Received JSON body:\n{json.dumps(kwargs['json'], indent=2)}")
                except Exception:
                    LOGGER.warning(f"[DEBUG CLIENT.REQUEST] 📦 Received JSON body (non-serializable): {kwargs['json']}")
            else:
                LOGGER.warning("[DEBUG CLIENT.REQUEST] ⚠️ No JSON body received by Client.request()")

            self.next_request_at = datetime.now() + TIME_BETWEEN_REQUESTS
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        # 🧭 Clean Debug Logging — No Prefixes
        req_body = kwargs.get("json")
        req_params = kwargs.get("params")

        if req_body:
            # Log the actual JQL payload being sent
            log_payload = {
                k: v for k, v in req_body.items()
                if k in ("jql", "startAt", "maxResults", "validateQuery", "fields", "expand")
            }
        elif req_params:
            # Log GET query params (unchanged)
            log_payload = {
                k: v for k, v in req_params.items()
                if k in ("startAt", "maxResults", "fields", "expand")
            }
        else:
            log_payload = {}

        LOGGER.warning(
            f"[DEBUG REQUEST] stream={tap_stream_id} | method={method} | path={path} | "
            f"status={response.status_code} | payload={log_payload}"
        )

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
        LOGGER.info(f"[DEBUG] Fetching detailed issue {issue_id_or_key} with fields={fields}, expand={expand}")
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
            LOGGER.info("Starting new login timer")
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
            LOGGER.info(f"Fetching Jira page (GET): startAt={params['startAt']}, maxResults={params['maxResults']}")

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
    This paginator *only* handles fetching issue IDs/keys from the search endpoint.
    Detailed issue fetching is handled separately.
    """

    def pages(self, *args, **kwargs):
        # The 'json' argument here should only contain 'jql', 'startAt', 'maxResults', 'validateQuery'
        # Do NOT include 'fields' or 'expand' here.
        # ✅ Sanitize kwargs to avoid legacy paginator pollution
        # Remove any unexpected prefixed keys that break Jira Cloud 2025+ API
        for bad_key in list(kwargs.keys()):
            if bad_key.startswith("jql_") or bad_key.startswith("issues_") or bad_key.startswith("body_"):
                LOGGER.debug(f"[DEBUG PAGINATION] 🧹 Removing legacy-prefixed key from kwargs: {bad_key}")
                kwargs.pop(bad_key, None)

        # ✅ Normalize: support either 'body' or 'json' keyword from caller
        if "body" in kwargs and not kwargs.get("json"):
            body_template = kwargs["body"].copy()
        elif "json" in kwargs:
            body_template = kwargs["json"].copy()
        else:
            body_template = {}

        # Safe defaults
        start_at = body_template.get("startAt", 0)
        max_results = body_template.get("maxResults", 50)
        total_pages = 0
        has_more_pages = True


        while has_more_pages:
            total_pages += 1
            # Jira Cloud 2025+ requires wrapping body in "searchRequest" and POSTing to /rest/api/3/search/jql
            # Jira Cloud 2025+ (confirmed via cURL) requires POST /rest/api/3/search/jql with flat JSON body
            body = {
                "searchRequest": {
                    "jql": body_template.get("jql") or "ORDER BY updated ASC",
                    "validateQuery": "strict",
                    "startAt": start_at,
                    "maxResults": max_results,
                    # Explicitly remove fields/expand — not supported in POST body
                    **{k: v for k, v in body_template.items() if k not in ["fields", "expand"]}
                }
            }

            LOGGER.info(f"[DEBUG PAGINATION] 🔄 Sending POST /rest/api/3/search/jql with startAt={start_at}, maxResults={max_results}")

            response = self.client.request(
                "issues", "POST", "/rest/api/3/search/jql", json=body
            )
            LOGGER.info(f"[DEBUG PAGINATION] 🧾 Final payload JSON → {json.dumps(body, indent=2)}")


            if not response:
                LOGGER.warning("[DEBUG PAGINATION] ⚠️ Empty response for JQL search; stopping pagination.")
                break

            # The /search/jql endpoint returns issues under the "issues" key
            page = response.get("issues", [])
            page_size = len(page)

            LOGGER.info(
                f"[DEBUG PAGINATION] 📄 Page {total_pages} → startAt={response.get('startAt', start_at)} "
                f"isLast={response.get('isLast')} total={response.get('total', 'unknown')} page_size={page_size}"
            )

            if not page:
                LOGGER.info("[DEBUG PAGINATION] ❌ No issues in this JQL search page; stopping.")
                break

            # isLast is the correct way to check for the last page for this endpoint
            has_more_pages = not response.get("isLast", False)
            start_at = response.get("startAt", start_at) + page_size # Use response.get('startAt') if available

            yield page