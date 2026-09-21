"""Every field a graph entity carries must be in the schema the checks validate against.

`assert_graph_entity_matches` runs both the expected and the actual entity through a
YAML schema that rejects any field it does not name. So a field added to one of these
models without being added to its YAML file fails every connector graph check at once,
on the nightly, hours after it merged - which is what `owner_device_id` and
`owner_device_name` did to GitLab, Jira and Linear.

This compares the two directly, so the gap is named on the pull request instead.
"""

from __future__ import annotations

import pytest
from app.models.entities import (
    AppMetadata,
    AppRole,
    AppUserGroup,
    CodeFileRecord,
    FileRecord,
    LinkRecord,
    PullRequestRecord,
    RecordGroup,
    TicketRecord,
    WebpageRecord,
)
from pydantic import BaseModel

from validation.graph_entity_validator import (
    _ENTITY_SCHEMA_LAYERS,
    merged_graph_entity_schema,
)

pytestmark = pytest.mark.unit

MODEL_FOR_ENTITY: dict[str, type[BaseModel]] = {
    "ticket_record": TicketRecord,
    "file_record": FileRecord,
    "pull_request_record": PullRequestRecord,
    "code_file_record": CodeFileRecord,
    "link_record": LinkRecord,
    "webpage_record": WebpageRecord,
    "record_group": RecordGroup,
    "app_user_group": AppUserGroup,
    "app_role": AppRole,
    "app_metadata": AppMetadata,
}


def test_every_entity_kind_has_a_model() -> None:
    """A new entity kind without a model here would go unchecked and look checked."""
    assert sorted(MODEL_FOR_ENTITY) == sorted(_ENTITY_SCHEMA_LAYERS)


@pytest.mark.parametrize("entity", sorted(MODEL_FOR_ENTITY))
def test_the_schema_names_every_field_the_model_carries(entity: str) -> None:
    model = MODEL_FOR_ENTITY[entity]
    declared = set(merged_graph_entity_schema(entity).fields)
    # Dumped, not read off `model_fields`: a field marked `exclude=True` - such as
    # `Record.location`, which is built for LLM context and never stored - is a real
    # field that never reaches the schema, and reading the model would demand a YAML
    # entry for it.
    carried = set(model.model_construct().model_dump(mode="json"))

    missing = sorted(carried - declared)
    assert not missing, (
        f"{model.__name__} carries {missing}, which "
        f"integration-tests/validation/schemas/{_ENTITY_SCHEMA_LAYERS[entity][-1]} "
        f"does not name; add each one before shipping it"
    )
