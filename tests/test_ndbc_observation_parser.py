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


@pytest.mark.parametrize(
    "invalid_timeout", [True, float("nan"), float("inf"), 0, -1, "bad"]
)
@pytest.mark.parametrize(
    "parser", [utils.parse_realtime2_data, utils.parse_5day2_data]
)
def test_ndbc_parsers_replace_invalid_request_timeout(
    monkeypatch, parser, invalid_timeout
):
    captured = {}

    def get(_url, timeout, headers):
        captured.update(timeout=timeout, headers=headers)
        return _Response(_current_ndbc_response())

    monkeypatch.setattr(utils.requests, "get", get)

    assert parser("44009", request_timeout=invalid_timeout) is not None
    assert captured["timeout"] == utils.NDBC_REQUEST_TIMEOUT_SECONDS


@pytest.mark.parametrize(
    "invalid_station_id",
    [
        None,
        "",
        "   ",
        True,
        pytest.param(float("nan"), id="nan"),
        "../44009",
        "44009?x=1",
        "44009/extra",
    ],
)
@pytest.mark.parametrize(
    "parser", [utils.parse_realtime2_data, utils.parse_5day2_data]
)
def test_ndbc_parsers_reject_invalid_station_identifier(
    monkeypatch, parser, invalid_station_id
):
    monkeypatch.setattr(
        utils.requests,
        "get",
        lambda *_args, **_kwargs: pytest.fail("provider called"),
    )

    assert parser(invalid_station_id) is None


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


@pytest.mark.parametrize("value", ["NaN", "inf", "-inf", "MM", "bad"])
def test_ndbc_numeric_lookup_rejects_nonfinite_and_missing_values(value):
    assert utils._safe_float_lookup(["WSPD"], [value], "WSPD") is None


def test_ndbc_numeric_lookup_preserves_finite_values():
    assert utils._safe_float_lookup(["WSPD"], ["5.5"], "WSPD") == 5.5


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("WSPD", "-0.1"),
        ("WDIR", "-1"),
        ("WDIR", "361"),
        ("WVHT", "-0.1"),
    ],
)
def test_ndbc_parser_rejects_physically_invalid_measurements(field, value):
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    header = ["YY", "MM", "DD", "hh", "mm", "WDIR", "WSPD", "WVHT", "ATMP"]
    row = [
        str(now.year),
        f"{now.month:02d}",
        f"{now.day:02d}",
        f"{now.hour:02d}",
        f"{now.minute:02d}",
        "180",
        "5.5",
        "1.2",
        "20.3",
    ]
    row[header.index(field)] = value

    result = utils._parse_data_row(header, row)

    output_field = {
        "WSPD": "wind_speed",
        "WDIR": "wind_direction",
        "WVHT": "wave_height",
    }[field]
    assert result[output_field] is None


@pytest.mark.parametrize("direction", ["0", "360"])
def test_ndbc_parser_preserves_direction_boundaries(direction):
    assert utils._safe_float_lookup(
        ["WDIR"], [direction], "WDIR", minimum=0, maximum=360
    ) == float(direction)
