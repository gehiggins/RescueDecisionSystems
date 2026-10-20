"""Offline regression coverage for NDBC observation text parsing."""

from datetime import datetime, timedelta, timezone

import pytest

from flask_app.app import utils


class _Response:
    def __init__(self, text):
        self.text = text

    @staticmethod
    def raise_for_status():
        return None


def _current_ndbc_response():
    now = datetime.now(timezone.utc)
    observation = (
        f"{now.year} {now.month:02d} {now.day:02d} "
        f"{now.hour:02d} {now.minute:02d} "
        "180 5.5 MM 1.2 MM MM MM MM 20.3"
    )
    return "\n".join(
        [
            "#YY MM DD hh mm WDIR WSPD GST WVHT DPD APD MWD PRES ATMP",
            "#yr mo dy hr mn degT m/s m/s m sec sec deg hPa degC",
            observation,
        ]
    )


@pytest.mark.parametrize(
    ("parser", "path_fragment"),
    [
        (utils.parse_realtime2_data, "/realtime2/44009.txt"),
        (utils.parse_5day2_data, "/5day2/44009_5day.txt"),
    ],
)
def test_ndbc_parsers_use_field_header_and_bounded_request(
    monkeypatch, parser, path_fragment
):
    captured = {}

    def get(url, timeout, headers):
        captured.update(url=url, timeout=timeout, headers=headers)
        return _Response(_current_ndbc_response())

    monkeypatch.setattr(utils.requests, "get", get)

    result = parser("44009", request_timeout=4)

    assert path_fragment in captured["url"]
    assert captured["timeout"] == 4
    assert captured["headers"]["User-Agent"] == "RDS-WEATHER/1.0"
    assert result["temperature"] == pytest.approx(20.3)
    assert result["wind_speed"] == pytest.approx(5.5)
    assert result["wind_direction"] == pytest.approx(180.0)
    assert result["wave_height"] == pytest.approx(1.2)


def _ndbc_row(observation_time, wind_speed):
    return [
        str(observation_time.year),
        f"{observation_time.month:02d}",
        f"{observation_time.day:02d}",
        f"{observation_time.hour:02d}",
        f"{observation_time.minute:02d}",
        "180",
        str(wind_speed),
    ]


def test_row_selection_rejects_stale_and_future_observations():
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    current = _ndbc_row(now - timedelta(minutes=30), 5.5)
    stale = _ndbc_row(now - timedelta(hours=2), 9.9)
    future = _ndbc_row(now + timedelta(hours=2), 8.8)

    result = utils._select_best_data_row([stale, future, current])

    assert result == current


@pytest.mark.parametrize(
    "observation_time",
    [
        datetime.now(timezone.utc).replace(second=0, microsecond=0)
        - timedelta(hours=2),
        datetime.now(timezone.utc).replace(second=0, microsecond=0)
        + timedelta(hours=2),
    ],
)
def test_row_selection_returns_none_when_no_current_observation(
    observation_time,
):
    assert utils._select_best_data_row(
        [_ndbc_row(observation_time, 5.5)]
    ) is None
