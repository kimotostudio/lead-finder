import re
from pathlib import Path

from tools.lead_supply_quality import (
    evaluate_row,
    load_config,
    rank_rows,
    synthetic_fixture_rows,
)
from tools.run_fukuoka_city_search import build_fukuoka_queries


CONFIG_PATH = Path("config/search_terms_fukuoka_gate_passable_services.json")


def _has_positive_token(query: str, token: str) -> bool:
    for match in re.finditer(re.escape(token.lower()), query.lower()):
        segment_start = max(query.rfind(sep, 0, match.start()) for sep in (" ", "\t", "\n", "\r")) + 1
        segment_prefix = query[segment_start : match.start()].strip()
        if not segment_prefix.startswith(("-", "−")):
            return True
    return False


def test_gate_passable_query_config_is_hard_geo_and_contact_pivoted() -> None:
    config = load_config(CONFIG_PATH)
    queries = build_fukuoka_queries(config, max_queries=100)

    assert len(queries) >= 15
    assert all("福岡市" in query for query in queries)
    assert all(any(token in query for token in config["contact_intent"]["prefer"]) for query in queries)

    for query in queries:
        for token in config["contact_intent"]["avoid"]:
            assert not _has_positive_token(query, token)
        for token in config["target_categories"]["exclude"]:
            assert not _has_positive_token(query, token)

    query_text = "\n".join(queries)
    for category in ["工務店", "リフォーム", "電気工事", "行政書士", "司法書士", "税理士", "看板製作", "印刷会社"]:
        assert category in query_text
    assert "福岡市中央区 行政書士事務所" in queries[0]
    assert "-site:jimdofree.com" in query_text
    assert "-site:amebaownd.com" in query_text
    assert "-site:*.xyz" in query_text


def test_supply_quality_routes_gate_blockers_away_with_synthetic_rows() -> None:
    config = load_config(CONFIG_PATH)
    evaluations = rank_rows(
        synthetic_fixture_rows(),
        config,
        ledger_ids={"ledger-row"},
        ledger_domains={"ledger-office.example.jp"},
    )
    by_id = {item.lead_id: item for item in evaluations}

    assert evaluations[0].lead_id == "own-contact"
    assert by_id["own-contact"].action == "allow"
    assert by_id["own-contact"].source_preference == "own_domain_same_site_contact"

    assert by_id["reservation-salon"].action == "exclude"
    assert any(reason.startswith("reservation_contact") for reason in by_id["reservation-salon"].reasons)
    assert any(reason.startswith("excluded_category") for reason in by_id["reservation-salon"].reasons)

    assert by_id["out-of-area"].action == "exclude"
    assert "missing_required_geo" in by_id["out-of-area"].reasons
    assert any(reason.startswith("out_of_area") for reason in by_id["out-of-area"].reasons)

    assert by_id["ledger-row"].action == "exclude"
    assert any(reason.startswith("already_in_ledger") for reason in by_id["ledger-row"].reasons)

    assert by_id["free-host"].action == "review"
    assert by_id["free-host"].source_preference == "free_host_deprioritized"
    assert by_id["own-contact"].source_score > by_id["free-host"].source_score

    assert by_id["aggregator-row"].action == "exclude"
    assert "source_aggregator" in by_id["aggregator-row"].reasons

    assert by_id["external-form"].action == "exclude"
    assert any(reason.startswith("external_contact_host") for reason in by_id["external-form"].reasons)


def test_ledger_exclusion_matches_domain_even_if_lead_id_is_new() -> None:
    config = load_config(CONFIG_PATH)
    row = {
        "lead_id": "new-id",
        "display_name": "福岡司法書士事務所",
        "website": "https://known-office.example.jp/",
        "contact_url": "https://known-office.example.jp/contact/",
        "business_type": "福岡市 司法書士 お問い合わせフォーム",
        "location": "福岡市",
    }

    evaluation = evaluate_row(row, config, ledger_domains={"known-office.example.jp"})

    assert evaluation.action == "exclude"
    assert "already_in_ledger:domain" in evaluation.reasons
