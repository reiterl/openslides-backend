from typing import Iterable

from ...models.models import AgendaItem
from ...shared.filters import FilterOperator
from ...shared.interfaces import Event, WriteRequestElement
from ...shared.patterns import FullQualifiedId
from ..base import Action, ActionPayload, DataSet
from ..default_schema import DefaultSchema
from ..register import register_action
from .agenda_tree import AgendaTree


@register_action("agenda_item.numbering")
class AgendaItemNumbering(Action):
    """
    Action to number all public agenda items.
    """

    model = AgendaItem()
    schema = DefaultSchema(AgendaItem()).get_default_schema(["meeting_id"])

    def prepare_dataset(self, payload: ActionPayload) -> DataSet:
        # Overwrite parent prepare_dataset
        # Fetch all agenda items for this meeting from database.
        meeting_id = payload[0]["meeting_id"]
        agenda_items = self.database.filter(
            collection=self.model.collection,
            filter=FilterOperator("meeting_id", "=", meeting_id),
            mapped_fields=["item_number", "parent_id", "weight", "type"],
            lock_result=True,
        )

        # Build agenda tree and get new numbers
        # TODO: Use roman numbers and prefix from config.
        numeral_system = "arabic"
        agenda_number_prefix = None
        return DataSet(
            data=AgendaTree(agenda_items.values()).number_all(
                numeral_system=numeral_system, agenda_number_prefix=agenda_number_prefix
            )
        )

    def create_write_request_elements(
        self, dataset: DataSet
    ) -> Iterable[WriteRequestElement]:
        information = {}
        events = []
        for instance_id, item_number in dataset["data"].items():
            fqid = FullQualifiedId(self.model.collection, instance_id)
            information[fqid] = ["Object updated"]
            events.append(
                Event(type="update", fqid=fqid, fields={"item_number": item_number})
            )
        yield WriteRequestElement(
            events=events, information=information, user_id=self.user_id
        )
