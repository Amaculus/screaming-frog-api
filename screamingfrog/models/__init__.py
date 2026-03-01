from .diff import CrawlDiff, FieldChange, RedirectChange, StatusChange, TitleChange
from .internal import InternalPage
from .link import Link

__all__ = [
    "InternalPage",
    "Link",
    "CrawlDiff",
    "StatusChange",
    "TitleChange",
    "RedirectChange",
    "FieldChange",
]
