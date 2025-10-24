from datetime import datetime
from singer import utils, metadata
from .http import check_status


class Context:
    config = None
    state = None
    catalog = None
    client = None
    stream_map = {}

    @classmethod
    def get_catalog_entry(cls, stream_name):
        if not cls.stream_map:
            cls.stream_map = {s.tap_stream_id: s for s in cls.catalog.streams}
        return cls.stream_map.get(stream_name)

    @classmethod
    def is_selected(cls, stream_name):
        stream = cls.get_catalog_entry(stream_name)
        if not stream:
            return False
        stream_metadata = metadata.to_map(stream.metadata)
        return metadata.get(stream_metadata, (), 'selected')

    @classmethod
    def bookmarks(cls):
        if "bookmarks" not in cls.state:
            cls.state["bookmarks"] = {}
        return cls.state["bookmarks"]

    @classmethod
    def bookmark(cls, key):
        """
        Compatibility shim for older tap-jira code.

        Supports both string and list keys.
        Ensures nested dicts are created as needed.
        """
        bookmarks = cls.bookmarks()

        if isinstance(key, list):
            cur = bookmarks
            for k in key:
                if k not in cur or not isinstance(cur[k], dict):
                    cur[k] = {}
                cur = cur[k]
            return cur

        # Handle string keys
        if key not in bookmarks or not isinstance(bookmarks[key], dict):
            bookmarks[key] = {}
        return bookmarks[key]

    @classmethod
    def set_bookmark(cls, path, val):
        if isinstance(val, datetime):
            val = utils.strftime(val)
        cls.bookmark(path[:-1])[path[-1]] = val

    @classmethod
    def update_start_date_bookmark(cls, path):
        val = cls.bookmark(path)
        if not val:
            val = cls.config["start_date"]
            val = utils.strptime_to_utc(val)
            cls.set_bookmark(path, val)
        if isinstance(val, str):
            val = utils.strptime_to_utc(val)
        return val

    @classmethod
    def retrieve_timezone(cls):
        response = cls.client.send("GET", "/rest/api/3/myself")
        check_status(response)
        return response.json()["timeZone"]