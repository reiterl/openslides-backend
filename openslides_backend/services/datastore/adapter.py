from typing import Dict, List, Sequence, Union

from ...shared.filters import Filter
from ...shared.interfaces import LoggingModule, WriteRequestElement
from ...shared.patterns import Collection, FullQualifiedField, FullQualifiedId
from . import commands
from .http_engine import HTTPEngine as Engine
from .interface import Aggregate, Count, Found, PartialModel


class Adapter:
    """
    Adapter to connect to readable and writeable datastore.
    """

    # The key of this dictionary is a stringified FullQualifiedId or FullQualifiedField
    locked_fields: Dict[str, int]

    def __init__(self, engine: Engine, logging: LoggingModule) -> None:
        self.logger = logging.getLogger(__name__)
        self.engine = engine
        self.locked_fields = {}

    def get(
        self,
        fqid: FullQualifiedId,
        mapped_fields: List[str] = None,
        position: int = None,
        get_deleted_models: int = None,
    ) -> PartialModel:
        if position is not None or get_deleted_models is not None:
            raise NotImplementedError(
                "The keywords 'position' and 'get_deleted_models' are not supported yet."
            )
        command = commands.Get(fqid=fqid, mappedFields=mapped_fields)
        self.logger.debug(
            f"Start GET request to datastore with the following data: {command.data}"
        )
        response = self.engine.get(command)
        meta_position = response.get("meta_position")
        if meta_position is not None:
            self.update_locked_fields(fqid, meta_position)
        return response

    def get_many(
        self,
        get_many_requests: List[commands.GetManyRequest],
        mapped_fields: List[str] = None,
        position: int = None,
        get_deleted_models: int = None,
    ) -> Dict[Collection, Dict[int, PartialModel]]:
        if position is not None or get_deleted_models is not None:
            raise NotImplementedError(
                "The keywords 'position' and 'get_deleted_models' are not supported yet."
            )
        command = commands.GetMany(
            get_many_requests=get_many_requests,
            mapped_fields=mapped_fields,
            position=position,
            get_deleted_models=get_deleted_models,
        )
        self.logger.debug(
            f"Start GET_MANY request to datastore with the following data: {command.data}"
        )
        response = self.engine.get_many(command)
        result = {}
        for collection_str in response.keys():
            inner_result = {}
            collection = Collection(collection_str)
            for id_str, value in response[collection_str].items():
                instance_id = int(id_str)
                meta_position = value.get("meta_position")
                if meta_position is not None:
                    fqid = FullQualifiedId(collection, instance_id)
                    self.update_locked_fields(fqid, meta_position)
                inner_result[instance_id] = value
            result[collection] = inner_result
        return result

    def get_all(
        self,
        collection: Collection,
        mapped_fields: List[str] = None,
        get_deleted_models: int = None,
    ) -> List[PartialModel]:
        if get_deleted_models is not None:
            raise NotImplementedError(
                "The keyword 'get_deleted_models' is not supported yet."
            )
        # TODO: Check the return value of this method. The interface docs say
        # something else.
        command = commands.GetAll(collection=collection, mapped_fields=mapped_fields)
        self.logger.debug(
            f"Start GET_ALL request to datastore with the following data: {command.data}"
        )
        response = self.engine.get_all(command)
        for item in response:
            meta_position = item.get("meta_position")
            item_id = item.get("id")
            if meta_position is not None and id is not None:
                fqid = FullQualifiedId(collection=collection, id=item_id)
                self.update_locked_fields(fqid, meta_position)
        return response

    def filter(
        self,
        collection: Collection,
        filter: Filter,
        meeting_id: int = None,
        mapped_fields: List[str] = None,
    ) -> List[PartialModel]:
        if meeting_id is not None or mapped_fields is not None:
            raise NotImplementedError(
                "The keywords 'meeting_id' and 'mapped_fields' are not supported yet."
            )
        # TODO: Check the return value of this method. The interface docs say
        # something else.
        command = commands.Filter(collection=collection, filter=filter)
        self.logger.debug(
            f"Start FILTER request to datastore with the following data: {command.data}"
        )
        response = self.engine.filter(command)
        for item in response:
            meta_position = item.get("meta_position")
            item_id = item.get("id")
            if meta_position is not None and id is not None:
                fqid = FullQualifiedId(collection=collection, id=item_id)
                self.update_locked_fields(fqid, meta_position)
        return response

    def exists(self, collection: Collection, filter: Filter) -> Found:
        # Attention: We do not handle the position result of this request. You
        # have to do this manually.
        command = commands.Exists(collection=collection, filter=filter)
        self.logger.debug(
            f"Start EXISTS request to datastore with the following data: {command.data}"
        )
        response = self.engine.exists(command)
        return {"exists": response["exists"], "position": response["position"]}

    def count(self, collection: Collection, filter: Filter) -> Count:
        # Attention: We do not handle the position result of this request. You
        # have to do this manually.
        command = commands.Count(collection=collection, filter=filter)
        self.logger.debug(
            f"Start COUNT request to datastore with the following data: {command.data}"
        )
        response = self.engine.count(command)
        return {"count": response["count"], "position": response["position"]}

    def min(
        self, collection: Collection, filter: Filter, field: str, type: str = None
    ) -> Aggregate:
        # TODO: This method does not reflect the position of the fetched objects.
        command = commands.Min(
            collection=collection, filter=filter, field=field, type=type
        )
        self.logger.debug(
            f"Start MIN request to datastore with the following data: {command.data}"
        )
        response = self.engine.min(command)
        return response

    def max(
        self, collection: Collection, filter: Filter, field: str, type: str = None
    ) -> Aggregate:
        # TODO: This method does not reflect the position of the fetched objects.
        command = commands.Max(
            collection=collection, filter=filter, field=field, type=type
        )
        self.logger.debug(
            f"Start MAX request to datastore with the following data: {command.data}"
        )
        response = self.engine.max(command)
        return response

    def update_locked_fields(
        self, key: Union[FullQualifiedId, FullQualifiedField], position: int,
    ) -> None:
        """
        Updates the locked_fields map by adding the new value for the given FQId or
        FQField. If there is an existing value we take the smaller one.
        """
        current_position = self.locked_fields.get(str(key))
        if current_position is None:
            new_position = position
        else:
            new_position = min(position, current_position)
        self.locked_fields[str(key)] = new_position

    def reserve_ids(self, collection: Collection, amount: int) -> Sequence[int]:
        command = commands.ReserveIds(collection=collection, amount=amount)
        self.logger.debug(
            f"Start RESERVE_IDS request to datastore with the following data: "
            f"Collection: {collection}, Amount: {amount}"
        )
        response = self.engine.reserve_ids(command)
        return response.get("ids")

    def reserve_id(self, collection: Collection) -> int:
        return self.reserve_ids(collection=collection, amount=1)[0]

    def write(self, write_requests: Sequence[WriteRequestElement]) -> None:
        # TODO: Support multiple write_requests
        if len(write_requests) != 1:
            raise RuntimeError("Multiple or None write_requests not supported.")
        command = commands.Write(
            write_request=write_requests[0], locked_fields=self.locked_fields
        )
        self.logger.debug(
            f"Start WRITE request to datastore with the following data: "
            f"Write request: {write_requests[0]}"
        )
        self.engine.write(command)
