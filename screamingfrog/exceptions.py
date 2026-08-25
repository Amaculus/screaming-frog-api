"""Public exception types.

Kept out of the backend modules so a consumer can catch a failure without
importing a backend it does not otherwise use, and so the import does not pull
in the Derby/JPype stack.
"""

from __future__ import annotations


class PageSpeedAuditMissingError(RuntimeError):
    """An expected Lighthouse audit id was absent from every PSI payload.

    Almost certainly renamed or retired by a Spider/Lighthouse upgrade: the
    audit-id mapping in the backend is pinned to a Spider version, and Lighthouse
    returns every audit whether it passes or not, so absence means the id moved
    rather than that the page is healthy.
    """
