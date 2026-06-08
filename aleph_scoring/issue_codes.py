"""Diagnostic issue codes published for CRN operators.

Only the numeric values appear in the published JSON (to keep the payload
small). The names and descriptions here are the single source of truth and
should be mirrored in the operator documentation.

STABILITY CONTRACT: values are permanent. Never reuse or renumber a code once
it has been published; only add new ones. Ranges:
  1xxx  scoring reasons   -- why the aggregated score is low or zero
  2xxx  measurement codes -- why an individual probe failed this cycle
"""

from enum import IntEnum
from typing import Dict


class IssueCode(IntEnum):
    # 1xxx -- scoring reasons (aggregated over the scoring/stability window)
    NO_IPV4 = 1001
    NO_IPV6 = 1002
    IPV4_UNSTABLE = 1003
    IPV6_UNSTABLE = 1004

    # 2xxx -- per-measurement failures (this measurement cycle)
    DNS_IPV4_FAIL = 2001
    DNS_IPV6_FAIL = 2002
    IPV4_CHECK_FAILED = 2003
    IPV6_CHECK_FAILED = 2004
    VERSION_UNAVAILABLE = 2005
    DIAG_VM_UNREACHABLE = 2006
    FULL_CHECK_FAILED = 2007
    UNKNOWN_MEASUREMENT_ERROR = 2999


ISSUE_DESCRIPTIONS: Dict[IssueCode, str] = {
    IssueCode.NO_IPV4: "No IPv4 address observed for the node in the stability window.",
    IssueCode.NO_IPV6: "No IPv6 address observed for the node in the stability window.",
    IssueCode.IPV4_UNSTABLE: "The node's IPv4 address changed too many times.",
    IssueCode.IPV6_UNSTABLE: "The node's IPv6 /64 prefix changed too many times.",
    IssueCode.DNS_IPV4_FAIL: "The node hostname did not resolve to an IPv4 address.",
    IssueCode.DNS_IPV6_FAIL: "The node hostname did not resolve to an IPv6 address.",
    IssueCode.IPV4_CHECK_FAILED: "The IPv4 reachability check (about/login) failed.",
    IssueCode.IPV6_CHECK_FAILED: "The IPv6 reachability check (about/login) failed.",
    IssueCode.VERSION_UNAVAILABLE: "The aleph-vm version could not be read from the node.",
    IssueCode.DIAG_VM_UNREACHABLE: "The diagnostic VM did not respond.",
    IssueCode.FULL_CHECK_FAILED: "The status/check/fastapi probe failed.",
    IssueCode.UNKNOWN_MEASUREMENT_ERROR: (
        "An unexpected error occurred while measuring the node; see node logs."
    ),
}
