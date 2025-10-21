from dataclasses import replace
import json
import pytz
import singer
import dateparser
import os

from singer import metrics, utils, metadata, Transformer
# Import the new method fetch_issue_details from http.Client
from .http import Paginator, JiraNotFoundError, IssuesPaginator, Client

from .context import Context

DEFAULT_PAGE_SIZE = 50

def raise_if_bookmark_cannot_advance(worklogs):
    # Worklogs can only be queried with a `since` timestamp and
    # provides no way to page through the results. The `since`
    # timestamp has <=, not <, semantics. It also caps the response at
    # 1000 objects. Because of this, if we ever see a page of 1000
    # worklogs that all have the same `updated` timestamp, we cannot
    # tell whether we in fact got all the updates and so we need to
    # raise.
    #
    # That said, a page of 999 worklogs that all have the same
    # timestamp is fine. That just means that 999 worklogs were
    # updated at the same timestamp but that we did, in fact, get them
    # all.
    #
    # The behavior, then, always resyncs the latest `updated`
    # timestamp, no matter how many results are there. If you have 500
    # worklogs updated at T1 and 999 worklogs updated at T2 and
    # `last_updated` is set to T1, the first trip through this will
    # see 1000 items, 500 of which have `updated==T1` and 500 of which
    # have `updated==T2`. Then, `last_updated` is set to T2 and due to
    # the <= semantics, you grab the 999 T2 worklogs which passes this
    # function because there's less than 1000 worklogs of
    # `updated==T2`.
    #
    # OTOH, if you have 1 worklog with `updated==T1` and 1000 worklogs
    # with `updated==T2`, first trip you see 1 worklog at T1 and 999
    # at T2 which this code will think is fine, but second trip
    # through you'll see 1000 worklogs at T2 which will fail
    # validation (because we can't tell whether there would be more
    # that should've been returned).
    LOGGER.debug('Worklog page count: `%s`', len(worklogs))
    worklog_updatedes = [utils.strptime_to_utc(w['updated'])
                         for w in worklogs]
    min_updated = min(worklog_updatedes)
    max_updated = max(worklog_updatedes)
    LOGGER.debug('Worklog min updated: `%s`', min_updated)
    LOGGER.debug('Worklog max updated: `%s`', max_updated)
    if len(worklogs) == 1000 and min_updated == max_updated:
        raise Exception(("Worklogs bookmark can't safely advance."
                         "Every `updated` field is `{}`")
                        .format(worklog_updatedes[0]))


def sync_sub_streams(detailed_issue):
    """
    Syncs sub-streams (comments, changelogs, transitions) from a *single, detailed* issue object.
    """
    # Ensure fields exist before trying to pop or access
    fields = detailed_issue.get("fields", {})

    comments = fields.pop("comment", {}).get("comments")
    if comments and Context.is_selected(ISSUE_COMMENTS.tap_stream_id):
        for comment in comments:
            comment["issueId"] = detailed_issue["id"]
        ISSUE_COMMENTS.write_page(comments)

    changelogs = detailed_issue.pop("changelog", {}).get("histories")
    if changelogs and Context.is_selected(CHANGELOGS.tap_stream_id):
        changelogs_to_store = []
        interested_changelog_fields = set(["status", "priority", "CX Bug Escalation"])
        for changelog in changelogs:
            changelog["issueId"] = detailed_issue["id"]
            # just store changelogs of which fields we are interested in
            if len([item for item in changelog["items"] if item.get("field") in interested_changelog_fields]) > 0:
                changelogs_to_store.append(changelog)
        CHANGELOGS.write_page(changelogs_to_store)

    transitions = detailed_issue.pop("transitions")
    if transitions and Context.is_selected(ISSUE_TRANSITIONS.tap_stream_id):
        for transition in transitions:
            transition["issueId"] = detailed_issue["id"]
        ISSUE_TRANSITIONS.write_page(transitions)


def advance_bookmark(worklogs):
    raise_if_bookmark_cannot_advance(worklogs)
    new_last_updated = max(utils.strptime_to_utc(w["updated"])
                           for w in worklogs)
    return new_last_updated


LOGGER = singer.get_logger()


class Stream():
    """Information about and functions for syncing streams for the Jira API.

    Important class properties:

    :var tap_stream_id:
    :var pk_fields: A list of primary key fields
    :var indirect_stream: If True, this indicates the stream cannot be synced
    directly, but instead has its data generated via a separate stream."""
    def __init__(self, tap_stream_id, pk_fields, indirect_stream=False, path=None):
        self.tap_stream_id = tap_stream_id
        self.pk_fields = pk_fields
        # Only used to skip streams in the main sync function
        self.indirect_stream = indirect_stream
        self.path = path

    def __repr__(self):
        return "<Stream(" + self.tap_stream_id + ")>"

    def sync(self):
        # Default sync function, should be overridden by specific streams with pagination or custom logic
        page = Context.client.request(self.tap_stream_id, "GET", self.path)
        self.write_page(page)

    def write_page(self, page):
        stream = Context.get_catalog_entry(self.tap_stream_id)
        stream_metadata = metadata.to_map(stream.metadata)
        extraction_time = singer.utils.now()
        for rec in page:
            with Transformer() as transformer:
                rec = transformer.transform(rec, stream.schema.to_dict(), stream_metadata)
            singer.write_record(self.tap_stream_id, rec, time_extracted=extraction_time)
        with metrics.record_counter(self.tap_stream_id) as counter:
            counter.increment(len(page))

def update_user_date(page):
    """
    Transform date value to 'yyyy-mm-dd' format.
    API returns userReleaseDate and userStartDate always in the dd/mm/yyyy format where the month name is in Abbreviation form.
    Dateparser library handles locale value and converts Abbreviation month to number.
    For example, if userReleaseDate is 12/abr/2022 then we are converting it to 2022-04-12.
    Then, at the end singer-python will transform any DateTime to %Y-%m-%dT00:00:00Z format.

    All the locales are supported except following below locales,
    Chinese, Italia, Japanese, Korean, Polska, Brasil.
    """
    if page.get('userReleaseDate'):
        page['userReleaseDate'] = transform_user_date(page['userReleaseDate'])
    if page.get('userStartDate'):
        page['userStartDate'] = transform_user_date(page['userStartDate'])

    return page

class BoardsAgile(Stream):
    def sync(self):
        page_num_offset = [self.tap_stream_id, "offset", "page_num"]
        page_num = Context.bookmark(page_num_offset) or 0
        pager = Paginator(Context.client, items_key="values", page_num=page_num)
        for page in pager.pages(self.tap_stream_id
                                ,"GET"
                                , "/rest/agile/1.0/board"):
            self.write_page(page)
            Context.set_bookmark(page_num_offset, pager.next_page_num)
            singer.write_state(Context.state)
        Context.set_bookmark(page_num_offset, None)
        singer.write_state(Context.state)

class BoardsGreenhopper(Stream):
    def sync(self):
        # BOARDS endpoint
        if Context.is_selected(BOARDS.tap_stream_id):
            path = "/rest/greenhopper/1.0/rapidview"
            boards = Context.client.request(self.tap_stream_id, "GET", path)['views']
            self.write_page(boards)

        for board in boards:
            if Context.is_selected(VELOCITY.tap_stream_id):
                # VELOCITY endpoint
                board_id = str(board["id"])
                path = ("/rest/greenhopper/1.0/rapid/charts/velocity.json?rapidViewId=" + board_id)
                # get data from the Velocity endpoint
                velocity = Context.client.request(VELOCITY.tap_stream_id, "GET", path)

                if velocity.get("sprints"):
                    sprint_data = velocity["sprints"]

                    # per Sprint in the Sprint-section of the data, add the Board id, Estimated value & Completed value from the VelocityStatEntries-section
                    for sprint in sprint_data:
                        sprint_id = str(sprint["id"])
                        velocity_stats = {
                            "boardId": board["id"],
                            "velocityEstimated": velocity["velocityStatEntries"][sprint_id]["estimated"]["value"],
                            "velocityCompleted": velocity["velocityStatEntries"][sprint_id]["completed"]["value"]
                            }
                        sprint.update(velocity_stats)
                    VELOCITY.write_page(sprint_data)

            # SPRINTS endpoint
            if (Context.is_selected(SPRINTS.tap_stream_id) and board["sprintSupportEnabled"]):

                path = "/rest/agile/1.0/board/{}/sprint".format(board["id"])
                page_num_offset = [SPRINTS.tap_stream_id, "offset", "page_num"]
                page_num = Context.bookmark(page_num_offset) or 0
                pager = Paginator(Context.client, items_key="values", page_num=page_num)

                for page in pager.pages(SPRINTS.tap_stream_id, "GET", path):
                    # BoardId 242 of this API call somehow doesn't return the originBoardId, add it anyway
                    for record in page:
                        record['boardId'] = board_id
                    SPRINTS.write_page(page)
                    Context.set_bookmark(page_num_offset, pager.next_page_num)
                Context.set_bookmark(page_num_offset, None)

                # SPRINTREPORTS endpoint
                if (Context.is_selected(SPRINTREPORTS_ISSUESADDED.tap_stream_id) or Context.is_selected(SPRINTREPORTS_SCALAR.tap_stream_id)):
                    for sprint in sprint_data:
                        sprint_id = str(sprint['id'])
                        path = "/rest/greenhopper/1.0/rapid/charts/sprintreport?rapidViewId=" + board_id + "&sprintId=" + sprint_id
                        sprint_report = Context.client.request(SPRINTREPORTS_ISSUESADDED.tap_stream_id, "GET", path)

                        if (Context.is_selected(SPRINTREPORTS_SCALAR.tap_stream_id) and sprint_report["contents"]["puntedIssuesEstimateSum"]["text"] != 'null'):
                            scalarvalues_json = '[{"boardId": ' + board_id + ', "sprintId": ' + sprint_id + ', "puntedIssuesEstimateSum": ' + sprint_report["contents"]["puntedIssuesEstimateSum"]["text"] + '}]'
                            scalarvalues_dict = json.loads(scalarvalues_json)
                            SPRINTREPORTS_SCALAR.write_page(scalarvalues_dict)

                        # modify the issueKeysAddedDuringSprint output into something processable: change key into a value, and add the identifiers
                        issuekeys_added_during_sprint = sprint_report["contents"]["issueKeysAddedDuringSprint"]
                        if (Context.is_selected(SPRINTREPORTS_ISSUESADDED.tap_stream_id) and len(issuekeys_added_during_sprint)) != 0:
                            modify_json = json.dumps(issuekeys_added_during_sprint)
                            modify_json = modify_json.replace('{','[{"boardId":' + board_id + ', "sprintId": ' + sprint_id + ', "issueKeyAddedDuringSprint": ').replace(': true,','}, {"boardId":' + board_id + ', "sprintId": ' + sprint_id + ', "issueKeyAddedDuringSprint":').replace(': true}','}]')
                            modified_dict = json.loads(modify_json)
                            SPRINTREPORTS_ISSUESADDED.write_page(modified_dict)


                # SPRINTISSUES endpoint
                if Context.is_selected(SPRINTISSUES.tap_stream_id):
                    for sprint in sprint_data:
                        sprint_id = str(sprint['id'])
                        # let's not collect all the sprintissue data, otherwise one call tends to be 3MB+
                        path = "/rest/agile/1.0/board/" + board_id + "/sprint/" + sprint_id + "/issue?fields=fixVersions"

                        page_num_offset = [SPRINTISSUES.tap_stream_id, "offset", "page_num"]
                        page_num = Context.bookmark(page_num_offset) or 0
                        pager = Paginator(Context.client, items_key="issues", page_num=page_num)

                        for page in pager.pages(SPRINTISSUES.tap_stream_id, "GET", path):
                            # the calling parameters (the identifiers) are missing in the output, let's add those to all records
                            for record in page:
                                record['boardId'] = board_id
                                record['sprintId'] = sprint_id
                            SPRINTISSUES.write_page(page)
                            Context.set_bookmark(page_num_offset, pager.next_page_num)
                        Context.set_bookmark(page_num_offset, None)
            singer.write_state(Context.state)

class Projects(Stream):
    def sync_on_prem(self):
        """ Sync function for the on prem instances"""
        projects = Context.client.request(
            self.tap_stream_id, "GET", "/rest/api/3/project",
            params={"expand": "description,lead,url,projectKeys"})
        for project in projects:
            # The Jira documentation suggests that a "versions" key may appear
            # in the project, but from my testing that hasn't been the case
            # (even when projects do have versions). Since we are already
            # syncing versions separately, pop this key just in case it
            # appears.
            project.pop("versions", None)
        self.write_page(projects)
        if Context.is_selected(VERSIONS.tap_stream_id):
            for project in projects:
                path = "/rest/api/3/project/{}/version".format(project["id"])
                pager = Paginator(Context.client, order_by="sequence")
                for page in pager.pages(VERSIONS.tap_stream_id, "GET", path):
                    # Transform userReleaseDate and userStartDate values to 'yyyy-mm-dd' format.
                    for each_page in page:
                        each_page = update_user_date(each_page)
                    VERSIONS.write_page(page)
        if Context.is_selected(COMPONENTS.tap_stream_id):
            for project in projects:
                path = "/rest/api/3/project/{}/component".format(project["id"])
                pager = Paginator(Context.client)
                for page in pager.pages(COMPONENTS.tap_stream_id, "GET", path):
                    COMPONENTS.write_page(page)

    def sync_cloud(self):
        """ Sync function for the cloud instances"""
        offset = 0
        while True:
            params = {
                "expand": "description,lead,url,projectKeys",
                "maxResults": DEFAULT_PAGE_SIZE, # maximum number of results to fetch in a page.
                "startAt": offset #the offset to start at for the next page
            }
            # For Projects, use the /project/search endpoint which is paginated
            projects = Context.client.request(
                self.tap_stream_id, "GET", "/rest/api/3/project/search",
                params=params)
            for project in projects.get('values'):
                # The Jira documentation suggests that a "versions" key may appear
                # in the project, but from my testing that hasn't been the case
                # (even when projects do have versions). Since we are already
                # syncing versions separately, pop this key just in case it
                # appears.
                project.pop("versions", None)
            self.write_page(projects.get('values'))
            if Context.is_selected(VERSIONS.tap_stream_id):
                for project in projects.get('values'):
                    path = "/rest/api/3/project/{}/version".format(project["id"])
                    pager = Paginator(Context.client, order_by="sequence")
                    for page in pager.pages(VERSIONS.tap_stream_id, "GET", path):
                        # Transform userReleaseDate and userStartDate values to 'yyyy-mm-dd' format.
                        for each_page in page:
                            each_page = update_user_date(each_page)

                        VERSIONS.write_page(page)
            if Context.is_selected(COMPONENTS.tap_stream_id):
                for project in projects.get('values'):
                    path = "/rest/api/3/project/{}/component".format(project["id"])
                    pager = Paginator(Context.client)
                    for page in pager.pages(COMPONENTS.tap_stream_id, "GET", path):
                        COMPONENTS.write_page(page)

            # `isLast` corresponds to whether it is the last page or not.
            if projects.get("isLast"):
                break
            offset = offset + DEFAULT_PAGE_SIZE # next offset to start from

    def sync(self):
        # The documentation https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-projects/#api-rest-api-3-project-get
        # suggests that the rest/api/3/project endpoint would be deprecated from the version 3 and w could use project/search endpoint
        # which gives paginated response. However, the on prem servers doesn't allow working on the project/search endpoint. Hence for the cloud
        # instances, the new endpoint would be called which also suggests pagination, but for on prm instances the old endpoint would be called.
        # As we want to include both the cloud as well as the on-prem servers.
        if Context.client.is_on_prem_instance:
            self.sync_on_prem()
        else:
            self.sync_cloud()

class ProjectTypes(Stream):
    def sync(self):
        path = "/rest/api/3/project/type"
        types = Context.client.request(self.tap_stream_id, "GET", path)
        for type_ in types:
            type_.pop("icon")
        self.write_page(types)


class Users(Stream):
    def sync(self):
        max_results = 2 # This seems low for users. Consider increasing.

        if Context.config.get("groups"):
            groups = Context.config.get("groups").split(",")
        else:
            groups = ["jira-administrators",
                      "jira-software-users",
                      "jira-core-users",
                      "jira-users",
                      "users"]

        for group in groups:
            group = group.strip()
            try:
                params = {"groupname": group,
                          "maxResults": max_results,
                          "includeInactiveUsers": True}
                pager = Paginator(Context.client, items_key='values')
                for page in pager.pages(self.tap_stream_id, "GET",
                                        "/rest/api/3/group/member",
                                        params=params):
                    self.write_page(page)
            except JiraNotFoundError:
                LOGGER.info("Could not find group \"%s\", skipping", group)


class Issues(Stream):
    def sync(self):
        updated_bookmark = [self.tap_stream_id, "updated"]
        page_num_offset = [self.tap_stream_id, "offset", "page_num"]
        
        # Initialize state_value to None here to prevent UnboundLocalError
        state_value = None 

        # -------------------------------------------------------------
        # 🧩 Optional local testing override: manually load state file
        # -------------------------------------------------------------
        if os.getenv("LOCAL_STATE_DEBUG", "false").lower() == "true":
            state_path = os.getenv("TAP_JIRA_STATE_PATH", "jira.json")
            if os.path.exists(state_path):
                try:
                    with open(state_path, "r") as f:
                        state_data = json.load(f)
                        bookmarks = (
                            state_data.get("completed", {})
                            .get("singer_state", {})
                            .get("bookmarks", {})
                        )
                        if bookmarks and "issues" in bookmarks and "updated" in bookmarks["issues"]:
                            state_val = bookmarks["issues"]["updated"]
                            Context.set_bookmark(updated_bookmark, state_val)
                            Context.state["bookmarks"]["issues"]["updated"] = state_val
                            LOGGER.info(
                                f"[LOCAL DEBUG] Loaded state bookmark manually from {state_path}: {state_val}"
                            )
                        else:
                            LOGGER.warning(
                                f"[LOCAL DEBUG] No valid 'issues.updated' bookmark in {state_path}"
                            )
                except Exception as e:
                    LOGGER.warning(f"[LOCAL DEBUG] Could not load {state_path}: {e}")
            else:
                LOGGER.warning(f"[LOCAL DEBUG] State file {state_path} not found")

        # -------------------------------------------------------------
        # STEP 1: Resolve start_date (priority: env > state > config)
        # -------------------------------------------------------------
        last_updated = None
        source_used = None
        
        env_start_date_raw = os.getenv("TAP_JIRA_START_DATE") or os.getenv("tapJiraStartDate")

        if env_start_date_raw and env_start_date_raw.strip():
            env_start_date = env_start_date_raw.strip()
            LOGGER.info(f"✅ start_date explicitly provided via ENV: {env_start_date}")
        else:
            env_start_date = None
            LOGGER.info("🌫️ No TAP_JIRA_START_DATE override detected (empty or missing) — will check state next.")
        
        LOGGER.info(
            f"🔍 start_date resolution order → ENV={bool(env_start_date)}, "
            f"STATE={bool(Context.bookmark(updated_bookmark))}, "
            f"CONFIG={bool(Context.config.get('start_date'))}"
        )
  
        if env_start_date:
            LOGGER.info(f"Environment start_date explicitly provided: {env_start_date}")
            try:
                last_updated = utils.strptime_to_utc(env_start_date.strip())
                source_used = f"ENV VAR ({env_start_date})"
                LOGGER.info(f"✅ start_date source resolved from ENV VAR ({env_start_date})")
                LOGGER.info(f"🧭 Final resolved start_date={last_updated}")
            except Exception as e:
                LOGGER.warning(f"Invalid env TAP_JIRA_START_DATE: {env_start_date}. Error: {e}")

        if not last_updated:
            try:
                state_value = Context.bookmark(updated_bookmark)
            except Exception as e:
                LOGGER.warning(f"⚠️ Error retrieving Context.bookmark: {e}")

            if not state_value and isinstance(Context.state, dict) and "completed" in Context.state:
                try:
                    nested = (
                        Context.state.get("completed", {})
                        .get("singer_state", {})
                        .get("bookmarks", {})
                        .get("issues", {})
                        .get("updated")
                    )
                    if nested:
                        state_value = nested
                        LOGGER.info(f"📂 Loaded nested Meltano state bookmark: {state_value}")
                    else:
                        LOGGER.info("📂 No nested Meltano bookmark found under 'completed.singer_state'.")
                except Exception as e:
                    LOGGER.warning(f"⚠️ Could not parse nested Meltano state structure: {e}")

        if state_value:
            try:
                last_updated = utils.strptime_to_utc(str(state_value).strip())
                source_used = f"STATE ({state_value})"
                LOGGER.info(f"✅ start_date source resolved from STATE ({state_value})")
            except Exception as e:
                LOGGER.warning(f"Invalid state bookmark format: {state_value}. Error: {e}")


        if not last_updated:
            cfg_date = Context.config.get("start_date")
            if cfg_date:
                try:
                    last_updated = utils.strptime_to_utc(str(cfg_date).strip())
                    source_used = f"CONFIG ({cfg_date})"
                    LOGGER.info(f"✅ start_date source resolved from CONFIG ({cfg_date})")
                except Exception as e:
                    LOGGER.warning(f"Invalid config start_date: {cfg_date}. Error: {e}")

        if not last_updated:
            LOGGER.warning("No valid start_date found in env/state/config — falling back to 2021-01-01T00:00:00Z.")
            last_updated = utils.strptime_to_utc("2021-01-01T00:00:00Z")
            source_used = "DEFAULT FALLBACK (2021-01-01)"
            LOGGER.info(f"✅ start_date source resolved from DEFAULT FALLBACK (2021-01-01)")

        LOGGER.info(f"🏁 Final start_date UTC value: {last_updated}")
        LOGGER.info(f"✅ start_date source resolved from {source_used}")


        # -------------------------------------------------------------
        # STEP 2: Resolve optional end_date (env > config)
        # -------------------------------------------------------------
        end_date = None
        env_end_date = os.getenv("TAP_JIRA_END_DATE") or os.getenv("tapJiraEndDate")
        cfg_end_date = Context.config.get("end_date")

        if env_end_date and env_end_date.strip():
            try:
                end_date = utils.strptime_to_utc(env_end_date.strip())
                LOGGER.info(f"Using end_date from environment: {env_end_date}")
            except Exception as e:
                LOGGER.warning(f"Invalid TAP_JIRA_END_DATE: {env_end_date}. Error: {e}")
        elif cfg_end_date and str(cfg_end_date).strip():
            try:
                end_date = utils.strptime_to_utc(str(cfg_end_date).strip())
                LOGGER.info(f"Using end_date from config: {cfg_end_date}")
            except Exception as e:
                LOGGER.warning(f"Invalid config end_date: {cfg_end_date}. Error: {e}")

        # -------------------------------------------------------------
        # STEP 3: Format dates with timezone
        # -------------------------------------------------------------
        timezone = Context.retrieve_timezone()

        # 🕓 Use full seconds precision — Jira prefers ISO-like timestamps
        start_date_str = last_updated.astimezone(pytz.timezone(timezone)).strftime("%Y-%m-%d %H:%M:%S")
        end_date_str = (
            end_date.astimezone(pytz.timezone(timezone)).strftime("%Y-%m-%d %H:%M:%S")
            if end_date
            else None
        )

        LOGGER.info(f"🧭 Final resolved start_date_str={start_date_str}, end_date_str={end_date_str}")
        if not end_date_str:
            LOGGER.warning("⚠️ end_date_str is None — falling back to open-ended JQL (no upper bound).")

        # -------------------------------------------------------------
        # STEP 4: Build JQL for initial search (no fields/expand here)
        # -------------------------------------------------------------
        if not start_date_str:
            raise ValueError("❌ start_date_str cannot be None — JQL requires a valid starting timestamp.")

        if end_date_str:
            jql = f"updated >= '{start_date_str}' AND updated < '{end_date_str}' ORDER BY updated ASC"
        else:
            jql = f"updated >= '{start_date_str}' ORDER BY updated ASC"

        LOGGER.info(f"🧩 Using JQL query: {jql}")

        # Jira Cloud now REQUIRES a bounded query and string-based validateQuery
        jql_search_payload = {
            "jql": jql,
            "validateQuery": "strict",  # ✅ must be string, not boolean
            "maxResults": DEFAULT_PAGE_SIZE,  # ✅ no need to include startAt (Paginator handles it)
        }

        # -------------------------------------------------------------
        # STEP 5: Pagination and sync using IssuesPaginator (POST-based)
        # -------------------------------------------------------------
        page_num = Context.bookmark(page_num_offset) or 0
        pager = IssuesPaginator(Context.client, items_key="issues", page_num=page_num)

        # We will store the *final* list of fully detailed issues here before writing them.
        # This allows us to gather all sub-stream data as well.
        detailed_issues_to_write = []
        
        current_max_updated_timestamp = last_updated # Keep track of the highest 'updated' timestamp seen

        # Use the IssuesPaginator to get pages of *basic* issue data (ID, key, and minimal fields)
        for basic_issue_page in pager.pages(self.tap_stream_id, "POST", "/rest/api/3/search/jql", json=jql_search_payload):
            if not basic_issue_page:
                LOGGER.info("No basic issues returned for JQL search; continuing to next page or stopping.")
                continue
            
            LOGGER.info(f"Processing page with {len(basic_issue_page)} basic issue records for detail fetching.")

            for basic_issue in basic_issue_page:
                issue_id = basic_issue.get("id") or basic_issue.get("key")
                if not issue_id:
                    LOGGER.warning(f"Skipping issue with no ID/key in basic search result: {basic_issue}")
                    continue

                try:
                    # STEP 5a: Fetch full details for each issue using the new method
                    detailed_issue = Context.client.fetch_issue_details(issue_id, self.tap_stream_id)
                    
                    if not detailed_issue:
                        LOGGER.warning(f"No detailed data returned for issue {issue_id}. Skipping.")
                        continue

                    # STEP 5b: Sync sub-streams (comments, changelogs, transitions) from the detailed issue
                    sync_sub_streams(detailed_issue)

                    # Remove fields that might cause schema issues or are handled by sub-streams
                    detailed_issue["fields"].pop("worklog", None)
                    detailed_issue["fields"].pop("operations", None)
                    # The sub_streams function pops 'comment', 'changelog', 'transitions'
                    # if they are selected. Ensure they are removed from the main issue record
                    # before writing the main issue to avoid writing duplicate data if sub_streams
                    # are not selected, or if they were not popped due to being empty.
                    detailed_issue.pop("changelog", None) 
                    detailed_issue.pop("transitions", None)
                    if "comment" in detailed_issue.get("fields", {}):
                        detailed_issue["fields"].pop("comment", None)

                    detailed_issues_to_write.append(detailed_issue)

                    # Update bookmark candidate
                    if detailed_issue["fields"].get("updated"):
                        issue_updated_ts = utils.strptime_to_utc(detailed_issue["fields"]["updated"])
                        if issue_updated_ts > current_max_updated_timestamp:
                            current_max_updated_timestamp = issue_updated_ts

                except JiraNotFoundError:
                    LOGGER.warning(f"Detailed issue {issue_id} not found, skipping.")
                except Exception as e:
                    LOGGER.error(f"Error fetching or processing detailed issue {issue_id}: {e}")
                    # Decide if you want to fail the sync or just skip this issue.
                    # For now, we'll log and continue.

            # After processing all issues on the current page, write the collected detailed issues
            if detailed_issues_to_write:
                self.write_page(detailed_issues_to_write)
                detailed_issues_to_write = [] # Clear for the next page
            else:
                LOGGER.info("No detailed issues collected on this page to write.")

            Context.set_bookmark(page_num_offset, pager.next_page_num)
            singer.write_state(Context.state)

        # -------------------------------------------------------------
        # STEP 6: Finalize bookmarks
        # -------------------------------------------------------------
        Context.set_bookmark(page_num_offset, None) # Reset page_num for next full sync
        Context.set_bookmark(updated_bookmark, current_max_updated_timestamp) # Use the max timestamp observed
        singer.write_state(Context.state)
        LOGGER.info(f"Finished syncing issues up to: {current_max_updated_timestamp.isoformat()}")


class Worklogs(Stream):
    def _fetch_ids(self, last_updated):
        # since_ts uses millisecond precision
        since_ts = int(last_updated.timestamp()) * 1000
        return Context.client.request(
            self.tap_stream_id,
            "GET",
            "/rest/api/3/worklog/updated",
            params={"since": since_ts},
        )

    def _fetch_worklogs(self, ids):
        if not ids:
            return []
        return Context.client.request(
            self.tap_stream_id, "POST", "/rest/api/3/worklog/list",
            headers={"Content-Type": "application/json"},
            data=json.dumps({"ids": ids}),
        )

    def sync(self):
        updated_bookmark = [self.tap_stream_id, "updated"]
        last_updated = Context.update_start_date_bookmark(updated_bookmark)
        while True:
            ids_page = self._fetch_ids(last_updated)
            if not ids_page["values"]:
                break
            ids = [x["worklogId"] for x in ids_page["values"]]
            worklogs = self._fetch_worklogs(ids)

            # Grab last_updated before transform in write_page
            new_last_updated = advance_bookmark(worklogs)

            self.write_page(worklogs)

            last_updated = new_last_updated
            Context.set_bookmark(updated_bookmark, last_updated)
            singer.write_state(Context.state)
            # lastPage is a boolean value based on
            # https://developer.atlassian.com/cloud/jira/platform/rest/v3/?utm_source=%2Fcloud%2Fjira%2Fplatform%2Frest%2F&utm_medium=302#api-api-3-worklog-updated-get
            last_page = ids_page.get("lastPage")
            if last_page:
                break


VERSIONS = Stream("versions", ["id"], indirect_stream=True)
BOARDS = BoardsGreenhopper("boardsGreenhopper", ["id"])
VELOCITY = Stream("velocity", ["id","boardId"], indirect_stream=True)
SPRINTS = Stream("sprints", ["id","boardId"], indirect_stream=True)
SPRINTREPORTS_SCALAR = Stream("sprintreports_scalar",["sprintId","boardId"], indirect_stream=True)
SPRINTREPORTS_ISSUESADDED = Stream("sprintreports_issuesadded",["sprintId","boardId","issueKeyAddedDuringSprint"], indirect_stream=True)
SPRINTISSUES = Stream("sprintissues",["sprintId","boardId","id"], indirect_stream=True)
COMPONENTS = Stream("components", ["id"], indirect_stream=True)
ISSUES = Issues("issues", ["id"])
ISSUE_COMMENTS = Stream("issue_comments", ["id"], indirect_stream=True)
ISSUE_TRANSITIONS = Stream("issue_transitions", ["id","issueId"], # Composite primary key
                           indirect_stream=True)
PROJECTS = Projects("projects", ["id"])
CHANGELOGS = Stream("changelogs", ["id"], indirect_stream=True)

ALL_STREAMS = [
    PROJECTS,
    BOARDS,
    VELOCITY,
    SPRINTS,
    SPRINTREPORTS_SCALAR,
    SPRINTREPORTS_ISSUESADDED,
    SPRINTISSUES,
    VERSIONS,
    COMPONENTS,
    ProjectTypes("project_types", ["key"]),
    Stream("project_categories", ["id"], path="/rest/api/3/projectCategory"),
    Stream("resolutions", ["id"], path="/rest/api/3/resolution"),
    Stream("roles", ["id"], path="/rest/api/3/role"),
    Users("users", ["accountId"]),
    ISSUES,
    ISSUE_COMMENTS,
    CHANGELOGS,
    ISSUE_TRANSITIONS,
    Worklogs("worklogs", ["id"]),
]

ALL_STREAM_IDS = [s.tap_stream_id for s in ALL_STREAMS]


class DependencyException(Exception):
    pass


def validate_dependencies():
    errs = []
    selected = [s.tap_stream_id for s in Context.catalog.streams
                if Context.is_selected(s.tap_stream_id)]
    msg_tmpl = ("Unable to extract {0} data. "
                "To receive {0} data, you also need to select {1}.")
    if VERSIONS.tap_stream_id in selected and PROJECTS.tap_stream_id not in selected:
        errs.append(msg_tmpl.format("Versions", "Projects"))
    if COMPONENTS.tap_stream_id in selected and PROJECTS.tap_stream_id not in selected:
        errs.append(msg_tmpl.format("Components", "Projects"))
    if BOARDS.tap_stream_id not in selected:
        if VELOCITY.tap_stream_id in selected:
            errs.append(msg_tmpl.format("Velocity", "boardsGreenhopper"))
        if SPRINTS.tap_stream_id in selected:
            errs.append(msg_tmpl.format("Sprints", "boardsGreenhopper"))
        if SPRINTREPORTS_SCALAR.tap_stream_id in selected:
            errs.append(msg_tmpl.format("Sprintreports Scalar", "boardsGreenhopper"))
        if SPRINTREPORTS_ISSUESADDED.tap_stream_id in selected:
            errs.append(msg_tmpl.format("Sprintreports IssuesAdded", "boardsGreenhopper"))
        if SPRINTISSUES.tap_stream_id in selected:
            errs.append(msg_tmpl.format("Sprint Issues", "boardsGreenhopper"))
    if ISSUES.tap_stream_id not in selected:
        if CHANGELOGS.tap_stream_id in selected:
            errs.append(msg_tmpl.format("Changelog", "Issues"))
        if ISSUE_COMMENTS.tap_stream_id in selected:
            errs.append(msg_tmpl.format("Issue Comments", "Issues"))
        if ISSUE_TRANSITIONS.tap_stream_id in selected:
            errs.append(msg_tmpl.format("Issue Transitions", "Issues"))
    if errs:
        raise DependencyException(" ".join(errs))

def transform_user_date(user_date):
    """
    Transform date value to 'yyyy-mm-dd' format.
    API returns userReleaseDate and userStartDate always in the dd/mm/yyyy format where the month name is in Abbreviation form.
    Dateparser library handles locale value and converts Abbreviation month to number.
    For example, if userReleaseDate is 12/abr/2022 then we are converting it to 2022-04-12.
    Then, at the end singer-python will transform any DateTime to %Y-%m-%dT00:00:00Z format.

    All the locales are supported except following below locales,
    Chinese, Italia, Japanese, Korean, Polska, Brasil.
    """
    return dateparser.parse(user_date).strftime('%Y-%m-%d')