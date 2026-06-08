import pytest
from urllib3.util import parse_url

from aleph_scoring import metrics
from aleph_scoring.issue_codes import IssueCode
from aleph_scoring.metrics import NodeInfo, crn_measurement_codes, get_crn_metrics


def _kwargs(**overrides):
    base = dict(
        ipv4="1.2.3.4",
        ipv6="2a01::1",
        version="1.0.0",
        base_latency=0.1,
        base_latency_ipv4=0.1,
        diagnostic_vm_latency=0.1,
        full_check_latency=0.1,
    )
    base.update(overrides)
    return base


@pytest.mark.parametrize(
    "overrides, expected",
    [
        ({}, []),  # everything succeeded -> no codes
        ({"ipv4": None}, [IssueCode.DNS_IPV4_FAIL]),
        ({"ipv6": None}, [IssueCode.DNS_IPV6_FAIL]),
        ({"base_latency_ipv4": None}, [IssueCode.IPV4_CHECK_FAILED]),
        ({"base_latency": None}, [IssueCode.IPV6_CHECK_FAILED]),
        ({"version": None}, [IssueCode.VERSION_UNAVAILABLE]),
        ({"diagnostic_vm_latency": None}, [IssueCode.DIAG_VM_UNREACHABLE]),
        ({"full_check_latency": None}, [IssueCode.FULL_CHECK_FAILED]),
    ],
)
def test_crn_measurement_codes_single(overrides, expected):
    assert crn_measurement_codes(**_kwargs(**overrides)) == expected


def test_crn_measurement_codes_all_failed():
    codes = crn_measurement_codes(
        ipv4=None,
        ipv6=None,
        version=None,
        base_latency=None,
        base_latency_ipv4=None,
        diagnostic_vm_latency=None,
        full_check_latency=None,
    )
    assert codes == [
        IssueCode.DNS_IPV4_FAIL,
        IssueCode.DNS_IPV6_FAIL,
        IssueCode.IPV4_CHECK_FAILED,
        IssueCode.IPV6_CHECK_FAILED,
        IssueCode.VERSION_UNAVAILABLE,
        IssueCode.DIAG_VM_UNREACHABLE,
        IssueCode.FULL_CHECK_FAILED,
    ]


@pytest.mark.asyncio
async def test_get_crn_metrics_reports_unexpected_error(monkeypatch):
    async def boom(*args, **kwargs):
        raise RuntimeError("kaboom")

    monkeypatch.setattr(metrics, "_measure_crn_metrics", boom)
    node = NodeInfo(url=parse_url("https://crn.example/"), hash="a" * 64)

    result = await get_crn_metrics(None, node, sessions=None)

    assert result.node_id == "a" * 64
    assert result.url == "https://crn.example/"
    assert result.codes == [int(IssueCode.UNKNOWN_MEASUREMENT_ERROR)]
    assert result.base_latency is None
