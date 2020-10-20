from typing import Any, Dict, List, Optional

from ...models.models import AgendaItem
from ...shared.exceptions import ActionException
from ...shared.filters import FilterOperator
from ...shared.patterns import FullQualifiedId
from ...shared.schema import schema_version
from ..base import ActionPayload
from ..generics import UpdateAction
from ..register import register_action


@register_action("agenda_item.assign")
class AgendaItemAssign(UpdateAction):
    """
    Action to assign agenda items.
    """

    model = AgendaItem()
    schema = {
        "$schema": schema_version,
        "title": "Agenda items assign new parent schema",
        "description": "An object containing an array of agenda item ids and the new parent id the items should be assigned to.",
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "ids": {
                    "description": "An array of agenda item ids where the items should be assigned to the new parent id.",
                    "type": "array",
                    "items": {"type": "integer"},
                    "minItems": 1,
                    "uniqueItems": True,
                },
                "parent_id": {
                    "description": "The agenda item id of the new parent item.",
                    "type": ["integer", "null"],
                },
                "meeting_id": {
                    "description": "The meeting id of the aganda_items.",
                    "type": "integer",
                },
            },
            "required": ["ids", "parent_id", "meeting_id"],
        },
        "minItems": 1,
    }

    def get_updated_instances(self, payload: ActionPayload) -> List[Dict[str, Any]]:
        return self.prepare_assign_data(
            parent_id=payload[0]["parent_id"],
            ids=payload[0]["ids"],
            meeting_id=payload[0]["meeting_id"],
        )

    def prepare_assign_data(
        self, parent_id: Optional[int], ids: List[int], meeting_id: int
    ) -> List[Dict[str, Any]]:
        filter = FilterOperator("meeting_id", "=", meeting_id)
        db_instances = self.database.filter(
            collection=self.model.collection,
            filter=filter,
            mapped_fields=["id"],
            lock_result=True,
        )
        updated_instances = []

        if parent_id is None:
            for id_ in ids:
                if id_ not in db_instances:
                    raise ActionException(f"Id {id_} not in db_instances.")
                updated_instances.append({"id": id_, "parent_id": None})
            return updated_instances

        # calc the ancesters of parent id
        ancesters = [parent_id]
        grandparent = self.database.get(
            FullQualifiedId(self.model.collection, parent_id), ["parent_id"]
        )
        while grandparent.get("parent_id") is not None:
            gp_parent_id = grandparent["parent_id"]
            ancesters.append(gp_parent_id)
            grandparent = self.database.get(
                FullQualifiedId(self.model.collection, gp_parent_id), ["parent_id"]
            )
        for id_ in ids:
            if id_ in ancesters:
                raise ActionException(
                    f"Assigning item {id_} to one of its children is not possible."
                )
            if id_ not in db_instances:
                raise ActionException(f"Id {id_} not in db_instances.")
            updated_instances.append({"id": id_, "parent_id": parent_id})
        return updated_instances
