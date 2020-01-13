from typing import Any, Dict, List, Tuple

from openslides_backend.adapters.filters import Filter, FilterOperator
from openslides_backend.general.patterns import Collection, FullQualifiedId

# Do not change order of this entries. Just append new ones.
TESTDATA = [
    {
        "collection": "mediafile_attachment",
        "id": 3549387598,
        "fields": {"meeting_ids": [], "topic_ids": []},
    },
    {
        "collection": "mediafile_attachment",
        "id": 7583920032,
        "fields": {"meeting_ids": [], "topic_ids": []},
    },
    {
        "collection": "topic",
        "id": 1312354708,
        "fields": {"meeting": 7816466305, "title": "title_Aevoozu3ua"},
    },
    {
        "collection": "mediafile_pubilc_file",
        "id": 9283748294,
        "fields": {"meeting": 4256427454},
    },
    {
        "collection": "meeting",
        "id": 2393342057,
        "fields": {"topic_ids": [], "user_ids": [5968705978, 4796568680]},
    },
    {
        "collection": "meeting",
        "id": 4002059810,
        "fields": {"topic_ids": [], "user_ids": [5968705978]},
    },
    {
        "collection": "meeting",
        "id": 3611987967,
        "fields": {"topic_ids": [], "user_ids": [5968705978]},
    },
]  # type: List[Dict[str, Any]]


class DatabaseTestAdapter:
    """
    Test adapter for database (read) queries.

    See openslides_backend.services.providers.DatabaseProvider for
    implementation.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def get(
        self, fqid: FullQualifiedId, mapped_fields: List[str] = None
    ) -> Tuple[Dict[str, Any], int]:
        result, position = self.getMany(fqid.collection, [fqid.id], mapped_fields)
        return result[fqid.id], position

    def getMany(
        self, collection: Collection, ids: List[int], mapped_fields: List[str] = None
    ) -> Tuple[Dict[int, Dict[str, Any]], int]:
        result = {}
        for data in TESTDATA:
            if data["collection"] == str(collection) and data["id"] in ids:
                element = {}
                if mapped_fields is None:
                    element = data["fields"]
                else:
                    for field in mapped_fields:
                        if field in data["fields"].keys():
                            element[field] = data["fields"][field]
                result[data["id"]] = element
        if len(ids) != len(result):
            # Something was not found.
            raise RuntimeError
        return (result, 1)

    def getId(self, collection: Collection) -> Tuple[int, int]:
        return (42, 1)

    def filter(
        self,
        collection: Collection,
        filter: Filter,
        meeting_id: int = None,
        mapped_fields: List[str] = None,
    ) -> Tuple[Dict[int, Dict[str, Any]], int]:
        result = {}
        for data in TESTDATA:
            data_meeting_id = data["fields"].get("meeting_id")
            if meeting_id is not None and (
                data_meeting_id is None or data_meeting_id != meeting_id
            ):
                continue
            if data["collection"] != str(collection):
                continue
            if not isinstance(filter, FilterOperator):
                # TODO: Implement other filters
                continue
            if (
                filter.operator == "=="
                and data["fields"].get(filter.field) == filter.value
            ):
                element = {}
                if mapped_fields is None:
                    element = data["fields"]
                else:
                    for field in mapped_fields:
                        if field in data["fields"].keys():
                            element[field] = data["fields"][field]
                result[data["id"]] = element
                continue
            # TODO: Implement other operators.
        return (result, 1)
