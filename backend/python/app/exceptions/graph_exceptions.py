class GraphDBError(Exception):
    """Base exception for graph database failures."""


class GraphQueryError(GraphDBError):
    """Raised when a graph read could not be completed.

    Exists so callers can tell "the query ran and matched nothing" from "the
    query never ran". Returning an empty list for both makes a database that is
    down, a malformed query, or an expired transaction indistinguishable from a
    normal, expected empty result, and callers act on it: safety gates report
    "nothing in flight", paging loops stop as if complete, and resets report a
    finished cleanup.
    """
