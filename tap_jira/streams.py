from dataclasses import replace
import json
import pytz
import singer
import dateparser
import os
import requests
from requests.auth import HTTPBasicAuth
from singer import metrics, utils, metadata, Transformer
from .http import Paginator, JiraNotFoundError, Client
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


def sync_sub_streams(page):
    enriched_issues = []
    
    # Add a counter to see how many issues we are processing
    # LOGGER.info(f"--- Enriching a page of {len(page)} issues ---")

    for i, issue in enumerate(page):
        try:
            full_issue_details = Context.client.fetch_issue_details(
                issue['id'], 
                expand="changelog,transitions"
            )

            # --- DIAGNOSTIC STEP 1: Check if 'expand' is working ---
            # This is the most important log. Does the response even contain the 'changelog' key?
            #LOGGER.info(f"Issue {issue['id']} (item {i+1}/{len(page)}): Full details received. Keys are: {list(full_issue_details.keys())}")

            changelogs = full_issue_details.get("changelog", {}).get("histories", [])
            
            # --- DIAGNOSTICS STEP 2: Check if we found any changelogs at all ---
            if changelogs:
                # LOGGER.info(f"Issue {issue['id']}: Found {len(changelogs)} total changelog entries.")
                
                changelogs_to_store = []
                interested_changelog_fields = set(["status", "priority", "CX Bug Escalation"])

                for changelog in changelogs:
                    changelog["issueId"] = full_issue_details["id"]
                    
                    # --- DIAGNOSTIC STEP 3: See the exact fields being changed ---
                    changed_fields = [item.get("field") for item in changelog.get("items", [])]
                    # LOGGER.info(f"Issue {issue['id']}, Changelog ID {changelog['id']}: Fields changed are: {changed_fields}")

                    if any(field in interested_changelog_fields for field in changed_fields):
                        # LOGGER.info(f"  ^^^ MATCH FOUND! Storing this changelog.")
                        changelogs_to_store.append(changelog)

                if changelogs_to_store:
                    CHANGELOGS.write_page(changelogs_to_store)
                    # LOGGER.info(f"Issue {issue['id']}: Wrote {len(changelogs_to_store)} matching changelogs to the stream.")
            else:
                LOGGER.info(f"Issue {issue['id']}: No changelogs found in the expanded details.")

            # (The rest of the function for transitions, comments, etc. remains the same)
            transitions = full_issue_details.get("transitions", [])
            if transitions and Context.is_selected(ISSUE_TRANSITIONS.tap_stream_id):
                for transition in transitions:
                    transition["issueId"] = full_issue_details["id"]
                ISSUE_TRANSITIONS.write_page(transitions)
            
            fields = full_issue_details.get("fields", {})
            if fields:
                comments = fields.get("comment", {}).get("comments", [])
                if comments and Context.is_selected(ISSUE_COMMENTS.tap_stream_id):
                    for comment in comments:
                        comment["issueId"] = full_issue_details["id"]
                    ISSUE_COMMENTS.write_page(comments)

            enriched_issues.append(full_issue_details)

        except Exception as e:
            # --- DIAGNOSTIC STEP 4: Catch ALL exceptions to see if something else is failing ---
            LOGGER.error(f"!!! An unexpected error occurred while enriching issue {issue['id']}: {e}", exc_info=True)
            continue
            
    return enriched_issues


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
    def sync(self):
        """
        This stream uses a direct, isolated requests session with Basic Auth
        to exactly mimic a successful curl command, bypassing the shared Client.
        """
        username = Context.config.get("username")
        password = Context.config.get("password")

        if not (username and password):
            LOGGER.warning("Username/Password not configured; skipping 'projects', 'versions', and 'components' streams.")
            return

        # --- THIS IS THE KEY CHANGE ---
        # Create a simple, dedicated session for this stream only.
        session = requests.Session()
        session.auth = (username, password) # Set Basic Auth directly
        session.headers.update({
            'Accept': 'application/json',
            'X-Atlassian-Token': 'no-check'
        })
        base_url = "https://degreedjira.atlassian.net/rest/api/3"
        # --- END KEY CHANGE ---

        all_projects = []
        try:
            # Use the dedicated session to fetch the project list.
            # This is a manual, non-paginated call for simplicity, assuming project list is not huge.
            # If it is, we can add simple startAt pagination here later.
            response = session.get(
                f"{base_url}/project/search",
                params={"expand": "description,lead,url,projectKeys"}
            )
            response.raise_for_status() # Will raise an error for 4xx/5xx status codes
            
            project_list = response.json().get("values", [])
            for project in project_list:
                project.pop("versions", None)
            
            self.write_page(project_list)
            all_projects.extend(project_list)
            LOGGER.info(f"Successfully fetched {len(all_projects)} projects.")

        except Exception as e:
            LOGGER.error(f"Failed to fetch the main project list. Error: {e}", exc_info=True)
            return

        for project in all_projects:
            if Context.is_selected(VERSIONS.tap_stream_id):
                path = f"/project/{project['id']}/version"
                try:
                    # Use the same session for the sub-stream call
                    response = session.get(f"{base_url}{path}")
                    response.raise_for_status()
                    page = response.json().get("values", [])
                    for record in page:
                        record = update_user_date(record)
                    VERSIONS.write_page(page)
                except Exception as e:
                    LOGGER.warning(f"Could not fetch versions for project {project['key']}. Error: {e}")
            
            if Context.is_selected(COMPONENTS.tap_stream_id):
                path = f"/project/{project['id']}/component"
                try:
                    # Use the same session for the sub-stream call
                    response = session.get(f"{base_url}{path}")
                    response.raise_for_status()
                    page = response.json().get("values", [])
                    COMPONENTS.write_page(page)
                except Exception as e:
                    LOGGER.warning(f"Could not fetch components for project {project['key']}. Error: {e}")

class ProjectTypes(Stream):
    def sync(self):
        path = "/rest/api/3/project/type"
        types = Context.client.request(self.tap_stream_id, "GET", path)
        for type_ in types:
            type_.pop("icon")
        self.write_page(types)


class Users(Stream):
    def sync(self):
        max_results = 2

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
        updated_bookmark_key = [self.tap_stream_id, "updated"]
        page_num_offset = [self.tap_stream_id, "offset", "page_num"]

        if os.getenv("LOCAL_STATE_DEBUG", "false").lower() == "true":
            state_path = os.getenv("TAP_JIRA_STATE_PATH", "jira.json")
            if os.path.exists(state_path):
                try:
                    with open(state_path, "r") as f:
                        state_data = json.load(f)
                        bookmarks = (state_data.get("completed", {}).get("singer_state", {}).get("bookmarks", {}))
                        if bookmarks and "issues" in bookmarks and "updated" in bookmarks["issues"]:
                            state_val = bookmarks["issues"]["updated"]
                            Context.set_bookmark(updated_bookmark_key, state_val)
                            if "bookmarks" not in Context.state: Context.state["bookmarks"] = {}
                            if "issues" not in Context.state["bookmarks"]: Context.state["bookmarks"]["issues"] = {}
                            Context.state["bookmarks"]["issues"]["updated"] = state_val
                except Exception:
                    # Silently ignore local debug errors
                    pass

        last_updated = None
        
        env_start_date = os.getenv("TAP_JIRA_START_DATE")
        if env_start_date and env_start_date.strip():
            try:
                last_updated = utils.strptime_to_utc(env_start_date)
            except Exception:
                pass # Silently ignore parse errors

        if not last_updated:
            state_value = None
            if isinstance(Context.state, dict):
                state_value = Context.state.get("bookmarks", {}).get("issues", {}).get("updated")
                if not state_value:
                    nested_val = (Context.state.get("completed", {}).get("singer_state", {}).get("bookmarks", {}).get("issues", {}).get("updated"))
                    if nested_val:
                        state_value = nested_val
            
            if state_value:
                try:
                    last_updated = utils.strptime_to_utc(state_value)
                except Exception:
                    pass # Silently ignore parse errors

        if not last_updated:
            config_date = Context.config.get("start_date")
            if config_date:
                try:
                    last_updated = utils.strptime_to_utc(config_date)
                except Exception:
                    pass # Silently ignore parse errors

        if not last_updated:
            fallback_date = "2021-01-01T00:00:00Z"
            last_updated = utils.strptime_to_utc(fallback_date)
        
        end_date = None
        env_end_date = os.getenv("TAP_JIRA_END_DATE")
        if env_end_date:
            try:
                end_date = utils.strptime_to_utc(env_end_date)
            except Exception:
                pass # Silently ignore parse errors

        timezone = Context.retrieve_timezone()
        start_date_str = last_updated.astimezone(pytz.timezone(timezone)).strftime("%Y-%m-%d %H:%M")
        end_date_str = (
            end_date.astimezone(pytz.timezone(timezone)).strftime("%Y-%m-%d %H:%M")
            if end_date
            else None
        )

        jql = (
            f'updated >= "{start_date_str}" AND updated < "{end_date_str}" order by updated asc'
            if end_date_str
            else f'updated >= "{start_date_str}" order by updated asc'
        )
        
        # This single log line is highly recommended to keep for operational visibility
        LOGGER.info(f"Syncing issues with JQL: {jql}")

        json_body = {
            "jql": jql,
            "maxResults": DEFAULT_PAGE_SIZE,
        }
        
        pager = Paginator(Context.client, items_key="issues")
        current_max_updated = last_updated

        for page in pager.pages(self.tap_stream_id, "POST", "/rest/api/3/search/jql", json=json_body):
            if not page:
                continue

            enriched_page = sync_sub_streams(page)
            
            if not enriched_page:
                continue

            for issue in enriched_page:
                fields = issue.get("fields", {})
                if fields:
                    fields.pop("worklog", None)
                    fields.pop("operations", None)
                    fields.pop("comment", None)

            if enriched_page[-1].get("fields", {}).get("updated"):
                page_max_updated = utils.strptime_to_utc(enriched_page[-1]["fields"]["updated"])
                if page_max_updated > current_max_updated:
                    current_max_updated = page_max_updated
            
            self.write_page(enriched_page)
            
            Context.set_bookmark(updated_bookmark_key, current_max_updated)
            singer.write_state(Context.state)

        Context.set_bookmark(page_num_offset, None)
        Context.set_bookmark(updated_bookmark_key, current_max_updated)
        singer.write_state(Context.state)


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