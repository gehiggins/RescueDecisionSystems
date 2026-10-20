import pandas as pd

from flask_app.app.finalize_alert_df import finalize_alert_df


def test_sequence_numbers_normalize_site_ids_and_advance_within_batch(caplog):
    incoming = pd.DataFrame(
        [
            {"site_id": "42", "detect_time": pd.Timestamp("2026-10-20T10:00:00Z")},
            {"site_id": "42", "detect_time": pd.Timestamp("2026-10-20T11:00:00Z")},
            {"site_id": "NEW", "detect_time": pd.Timestamp("2026-10-20T12:00:00Z")},
        ]
    )
    history = pd.DataFrame(
        [
            {"site_id": 42, "alert_sequence_number": 1},
            {"site_id": "42", "alert_sequence_number": "2"},
            {"site_id": "42", "alert_sequence_number": "bad"},
            {"site_id": "42", "alert_sequence_number": 2.5},
        ]
    )

    result = finalize_alert_df(incoming, history)

    assert result["alert_sequence_number"].tolist() == [3, 4, 1]
    assert str(result["alert_sequence_number"].dtype) == "Int64"
    assert "Ignored 2 invalid historical alert sequence value" in caplog.text


def test_missing_history_schema_starts_new_sequence(caplog):
    incoming = pd.DataFrame(
        [{"site_id": "A", "detect_time": pd.Timestamp("2026-10-20T10:00:00Z")}]
    )

    result = finalize_alert_df(incoming, pd.DataFrame({"unexpected": [1]}))

    assert result["alert_sequence_number"].tolist() == [1]
    assert "history lacks site_id or alert_sequence_number" in caplog.text


def test_missing_detect_time_column_is_added_without_mutating_input(caplog):
    incoming = pd.DataFrame([{"site_id": "A"}])

    result = finalize_alert_df(incoming, None)

    assert "detect_time" not in incoming.columns
    assert pd.isna(result.loc[0, "detect_time"])
    assert result.loc[0, "alert_sequence_number"] == 1
    assert "1 alert(s) are missing detect_time" in caplog.text


def test_missing_site_ids_still_advance_deterministically_within_batch():
    incoming = pd.DataFrame(
        [
            {"detect_time": pd.Timestamp("2026-10-20T10:00:00Z")},
            {"detect_time": pd.Timestamp("2026-10-20T11:00:00Z")},
        ]
    )

    result = finalize_alert_df(incoming, pd.DataFrame())

    assert result["alert_sequence_number"].tolist() == [1, 2]


def test_detect_times_are_normalized_to_utc_without_mutating_input():
    incoming = pd.DataFrame(
        [
            {"site_id": "A", "detect_time": "2026-10-20T03:00:00-07:00"},
            {"site_id": "B", "detect_time": pd.Timestamp("2026-10-20 11:00:00")},
        ]
    )

    result = finalize_alert_df(incoming, pd.DataFrame())

    assert incoming.loc[0, "detect_time"] == "2026-10-20T03:00:00-07:00"
    # Pandas 2 and 3 may choose different internal timestamp resolutions;
    # timezone awareness and values are the operational guarantees.
    assert pd.api.types.is_datetime64_any_dtype(result["detect_time"])
    assert str(result["detect_time"].dt.tz) == "UTC"
    assert result["detect_time"].tolist() == [
        pd.Timestamp("2026-10-20T10:00:00Z"),
        pd.Timestamp("2026-10-20T11:00:00Z"),
    ]


def test_invalid_detect_times_become_missing_and_are_reported(caplog):
    incoming = pd.DataFrame(
        [
            {"site_id": "A", "detect_time": "not-a-time"},
            {"site_id": "B", "detect_time": ["2026-10-20T10:00:00Z"]},
            {"site_id": "C", "detect_time": 12345},
            {"site_id": "D", "detect_time": None},
        ]
    )

    result = finalize_alert_df(incoming, pd.DataFrame())

    assert result["detect_time"].isna().all()
    assert "Rejected 3 invalid detect_time value(s)" in caplog.text
    assert "4 alert(s) are missing detect_time" in caplog.text
