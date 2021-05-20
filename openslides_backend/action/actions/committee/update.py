from typing import Any, Dict

from ....models.models import Committee
from ....permissions.management_levels import (
    CommitteeManagementLevel,
    OrganisationManagementLevel,
)
from ....permissions.permission_helper import (
    has_committee_management_level,
    has_organisation_management_level,
)
from ....shared.exceptions import MissingPermission, PermissionDenied
from ...generics.update import UpdateAction
from ...util.default_schema import DefaultSchema
from ...util.register import register_action


@register_action("committee.update")
class CommitteeUpdateAction(UpdateAction):
    """
    Action to update a committee.
    """

    model = Committee()
    schema = DefaultSchema(Committee()).get_update_schema(
        optional_properties=[
            "name",
            "description",
            "template_meeting_id",
            "default_meeting_id",
            "user_ids",
            "forward_to_committee_ids",
            "receive_forwardings_from_committee_ids",
            "organisation_tag_ids",
        ]
    )

    def check_permissions(self, instance: Dict[str, Any]) -> None:
        is_manager = has_committee_management_level(
            self.datastore,
            self.user_id,
            CommitteeManagementLevel.CAN_MANAGE,
            instance["id"],
        )
        can_manage_organisation = has_organisation_management_level(
            self.datastore,
            self.user_id,
            OrganisationManagementLevel.CAN_MANAGE_ORGANISATION,
        )
        if (
            any(
                [
                    field in instance
                    for field in [
                        "name",
                        "description",
                        "template_meeting_id",
                        "default_meeting_id",
                    ]
                ]
            )
            and not is_manager
        ):
            raise MissingPermission(CommitteeManagementLevel.CAN_MANAGE)
        if (
            any(
                [
                    field in instance
                    for field in [
                        "user_ids",
                        "forward_to_committee_ids",
                        "receive_forwardings_from_committee_ids",
                    ]
                ]
            )
            and not can_manage_organisation
        ):
            raise MissingPermission(OrganisationManagementLevel.CAN_MANAGE_ORGANISATION)
        if (
            "organisation_tag_ids" in instance
            and not is_manager
            and not can_manage_organisation
        ):
            raise PermissionDenied("Missing can_manage_organisation and not manager.")
