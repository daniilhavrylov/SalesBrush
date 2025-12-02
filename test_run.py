from datetime import date
from unittest.mock import patch, Mock

import pytest

from run import parse_date, interval_calculation, data_processing, update_data


# -------------------- parse_date --------------------
def test_parse_date():
    d = parse_date("2025-12-02")
    assert isinstance(d, date)
    assert d.year == 2025
    assert d.month == 12
    assert d.day == 2

# -------------------- interval_calculation --------------------
def test_interval_calculation():
    minutes = interval_calculation(max_requests_per_day=100)
    assert isinstance(minutes, float)
    assert abs(minutes - 18) < 0.1

    with pytest.raises(ValueError):
        interval_calculation(max_requests_per_day=0)

    with pytest.raises(ValueError):
        interval_calculation(max_requests_per_day=-10)

    minutes = interval_calculation(max_requests_per_day=10)
    assert minutes > 0

    minutes = interval_calculation(max_requests_per_day=1000)
    assert minutes > 0

# -------------------- data_processing --------------------
def test_data_processing():
    spend_data = [
        {"date": "2025-06-04", "campaign_id": "CAMP-123", "spend": 37.50},
        {"date": "2025-06-04", "campaign_id": "CAMP-456", "spend": 19.90},
        {"date": "2025-06-05", "campaign_id": "CAMP-123", "spend": 42.10},
        {"date": "2025-06-05", "campaign_id": "CAMP-789", "spend": 11.00},
        {"date": "2025-06-06", "campaign_id": "CAMP-999", "spend": 5.25}
    ]
    conv_data = [
        {"date": "2025-06-04", "campaign_id": "CAMP-123", "conversions": 14},
        {"date": "2025-06-04", "campaign_id": "CAMP-456", "conversions": 3},
        {"date": "2025-06-05", "campaign_id": "CAMP-123", "conversions": 10},
        {"date": "2025-06-05", "campaign_id": "CAMP-456", "conversions": 5},
        {"date": "2025-06-06", "campaign_id": "CAMP-888", "conversions": 7}
    ]

    start_date = parse_date("2025-06-04")
    end_date = parse_date("2025-06-06")

    results = data_processing(start_date, end_date, spend_data, conv_data)

    assert len(results) == 7

    r1 = next(r for r in results if r["campaign_id"] == "CAMP-123")
    assert r1["cpa"] == 4.21  # 42.10 / 10

    r2 = next(r for r in results if r["campaign_id"] == "CAMP-888")
    assert r2["cpa"] is None


def test_data_processing_no_conversions():
    spend_data = [
        {"date": "2025-06-04", "campaign_id": "CAMP-123", "spend": 100.0}
    ]
    conv_data = []

    results = data_processing(
        parse_date("2025-06-04"),
        parse_date("2025-06-04"),
        spend_data,
        conv_data
    )

    assert len(results) == 1
    assert results[0]["cpa"] is None
    assert results[0]["spend"] == 100.0
    assert results[0]["conversions"] == 0


# -------------------- update_data --------------------
def test_update_data_success():
    mock_repo = Mock()
    mock_spend = [{"date": "2025-01-01", "campaign_id": "TEST", "spend": 100}]
    mock_conv = [{"date": "2025-01-01", "campaign_id": "TEST", "conversions": 5}]

    with patch("run.request_api", return_value=(mock_spend, mock_conv)):
        update_data(mock_repo)

    mock_repo.upsert_stats.assert_called_once()
    result = mock_repo.upsert_stats.call_args[0][0]
    assert result[0]["cpa"] == 20  # 100 / 5



def test_update_data_empty_result():
    mock_repo = Mock()
    with patch("run.request_api", return_value=([], [])):
        update_data(mock_repo)

    mock_repo.upsert_stats.assert_not_called()

