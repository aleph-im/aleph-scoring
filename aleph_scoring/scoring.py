import itertools
import logging
import math
from collections import defaultdict
from datetime import datetime
from typing import Sequence, List, Dict, Optional, Iterable, Collection

import pandas as pd

from .config import settings
from .schemas.metrics import NodeMetrics, CcnMetrics, CrnMetrics, AlephNodeMetrics
from .schemas.scoring import NodeScores, CcnScore, CrnScore

LOGGER = logging.getLogger(__name__)


def score_latency(value: Optional[float]) -> float:
    if value is None:
        return 0.0
    value = max(min(value, value - 0.10), 0)  # Tolerance
    score = max(1 - value, 0)
    assert 0 <= score <= 1, f"Out of range {score} from {value}"
    return score


def score_pending_messages(value: Optional[float]):
    if value is None or math.isnan(value):
        value = 1_000_000
    value = min(int(value), 1_000_000)
    return 1 - (value / 1_000_000)


def score_eth_height_remaining(value: Optional[float]):
    if value is None or math.isnan(value):
        value = 100_000
    value = max(int(value), 0)
    return 1 - (value / 100_000)


def compute_ccn_score(df: pd.DataFrame):
    """Compute the global score of a Core Channel Node (CCN)"""

    scores = df.copy()

    scores["score_base_latency"] = df["base_latency"].fillna(10).apply(score_latency)
    scores["score_metrics_latency"] = (
        df["metrics_latency"].fillna(10).apply(score_latency)
    )
    scores["score_aggregate_latency"] = (
        df["aggregate_latency"].fillna(10).apply(score_latency)
    )
    scores["score_file_download_latency"] = (
        df["file_download_latency"].fillna(10).apply(score_latency)
    )
    scores["score_pending_messages"] = (
        df["pending_messages"].fillna(1_000_000).apply(score_pending_messages)
    )
    scores["score_eth_height_remaining"] = (
        df["eth_height_remaining"].fillna(100_000).apply(score_eth_height_remaining)
    )

    scores["score"] = (
        scores["score_base_latency"]
        * scores["score_metrics_latency"]
        * scores["score_aggregate_latency"]
        * scores["score_file_download_latency"]
        * scores["score_pending_messages"]
        # * scores["eth_height_remaining"]
    ) ** (1 / 5.0)
    return scores


def compute_crn_score(df: pd.DataFrame):
    """Compute the global score of a Core Channel Node (CCN)"""

    print(df)

    df = df.groupby("url").agg(
        {
            "base_latency": "mean",
            "diagnostic_vm_latency": "mean",
            "full_check_latency": "mean",
        }
    )

    df["score"] = (
        df["base_latency"]
        * df["diagnostic_vm_latency"]
        * df["full_check_latency"]
        / 1000
    )

    if settings.EXPORT_DATAFRAME:
        df.to_csv(f"exports/crn_score-{datetime.now()}")

    LOGGER.info("Finished processing crn score ")
    return df


def compute_asn_score(asn: Optional[int], nb_nodes: int, total_nb_nodes: int):
    if asn is None:
        return 0

    return 1 - nb_nodes / total_nb_nodes


def compute_decentralization_scores(
    metrics: Collection[AlephNodeMetrics],
) -> Dict[Optional[int], float]:
    asn_count_dict = defaultdict(int)
    for m in metrics:
        asn_count_dict[m.asn] += 1

    asn_scores = {
        asn: compute_asn_score(asn, n, len(metrics))
        for asn, n in asn_count_dict.items()
    }
    return asn_scores


def compute_ccn_score_no_pandas(
    ccn_metrics: CcnMetrics,
    decentralization_scores: Dict[Optional[int], float],
) -> CcnScore:
    score_base_latency = score_latency(ccn_metrics.base_latency)
    score_aggregate_latency = score_latency(ccn_metrics.aggregate_latency)
    score_file_download_latency = score_latency(ccn_metrics.file_download_latency)
    score_metrics_latency = score_latency(ccn_metrics.metrics_latency)
    score_eth_height = score_eth_height_remaining(ccn_metrics.eth_height_remaining)
    score_pending = score_pending_messages(ccn_metrics.pending_messages)

    total_score = (
        score_base_latency
        * score_aggregate_latency
        * score_file_download_latency
        * score_metrics_latency
        * score_eth_height
        * score_pending
    ) ** (1 / 6)

    return CcnScore(
        node_id=ccn_metrics.node_id,
        total_score=total_score,
        base_latency=score_base_latency,
        decentralization=decentralization_scores[ccn_metrics.asn],
        aggregate_latency=score_aggregate_latency,
        file_download_latency=score_file_download_latency,
        metrics_endpoint_latency=score_metrics_latency,
        eth_height_remaining=score_eth_height,
        pending_messages=score_pending,
    )


def compute_crn_score_no_pandas(
    crn_metrics: CrnMetrics,
    decentralization_scores: Dict[Optional[int], float],
) -> CrnScore:
    score_base_latency = score_latency(crn_metrics.base_latency)
    score_diagnostic_vm_latency = score_latency(crn_metrics.diagnostic_vm_latency)
    score_full_check_latency = score_latency(crn_metrics.full_check_latency)

    total_score = (
        score_base_latency * score_diagnostic_vm_latency * score_full_check_latency
    ) ** (1 / 3)

    return CrnScore(
        node_id=crn_metrics.node_id,
        total_score=total_score,
        base_latency=score_base_latency,
        decentralization=decentralization_scores[crn_metrics.asn],
        diagnostic_vm_latency=score_diagnostic_vm_latency,
        full_check_latency=score_full_check_latency,
    )


def compute_ccn_scores(ccn_metrics: Collection[CcnMetrics]) -> List[CcnScore]:
    decentralization_scores = compute_decentralization_scores(ccn_metrics)
    ccn_scores = [
        compute_ccn_score_no_pandas(metrics, decentralization_scores)
        for metrics in ccn_metrics
    ]
    return ccn_scores


def compute_crn_scores(crn_metrics: Collection[CrnMetrics]) -> List[CrnScore]:
    decentralization_scores = compute_decentralization_scores(crn_metrics)
    crn_scores = [
        compute_crn_score_no_pandas(metrics, decentralization_scores)
        for metrics in crn_metrics
    ]
    return crn_scores


def compute_scores(node_metrics: NodeMetrics) -> NodeScores:
    ccn_scores = compute_ccn_scores(node_metrics.ccn)
    crn_scores = compute_crn_scores(node_metrics.crn)

    return NodeScores(
        ccn=ccn_scores,
        crn=crn_scores,
    )
