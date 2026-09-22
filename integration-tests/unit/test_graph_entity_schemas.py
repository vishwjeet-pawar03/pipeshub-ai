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
    # `exclude=True` fields - `Record.location`, built for LLM context and never
    # stored - never reach the schema, so requiring a YAML entry for them would be
    # wrong. Everything else does: dumping a `model_construct()` instance instead
    # would drop every required field that has no default, which is most of the
    # identifying ones (`record_name`, `connector_id`, `is_file`, `url`).
    carried = {name for name, field in model.model_fields.items() if field.exclude is not True}

    # Every layer, not just the last: a field on the base `Record` belongs in
    # record.yaml, and naming only the leaf sends the reader to the wrong file.
    layers = ", ".join(
        f"integration-tests/validation/schemas/{name}"
        for name in _ENTITY_SCHEMA_LAYERS[entity]
    )
    missing = sorted(carried - declared)
    assert not missing, (
        f"{model.__name__} carries {missing}, which {layers} "
        f"does not name; add each one before shipping it"
    )
