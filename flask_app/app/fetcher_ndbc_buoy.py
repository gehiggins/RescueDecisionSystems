# fetcher_ndbc_buoy.py - Offshore Buoy Fetcher (2025-03-07 - Updated)

from app.setup_imports import *
from pathlib import Path
import math
import re

from flask_app.app.utils import (
    get_current_utc_timestamp,
    log_error_and_continue,
    parse_5day2_data,
    parse_realtime2_data,
)
from flask_app.app.utils_geo import haversine_nm
from flask_app.app.utils_weather import calculate_timelate

DEFAULT_DATA_FOLDER = Path(__file__).resolve().parents[2] / "data"


def _metadata_path():
    """Resolve buoy metadata from configuration or the repository data folder."""
    data_folder = Path(os.getenv("RDS_DATA_FOLDER", str(DEFAULT_DATA_FOLDER)))
    return data_folder / "reference" / "ndbc_station_metadata_full.csv"

def load_buoy_metadata():
    metadata_path = _metadata_path()
    try:
        logging.info(
            "%s Loading buoy metadata from: %s",
            get_current_utc_timestamp(),
            metadata_path,
        )
        return pd.read_csv(metadata_path)
    except Exception as e:
        log_error_and_continue(f"{get_current_utc_timestamp()} âŒ Failed to load buoy metadata: {e}")
        return pd.DataFrame()


def _coordinates_from_metadata(row):
    """Return coordinates from columns or embedded NDBC station text."""
    try:
        latitude = float(row.get("latitude"))
        longitude = float(row.get("longitude"))
        if (
            math.isfinite(latitude)
            and math.isfinite(longitude)
            and -90 <= latitude <= 90
            and -180 <= longitude <= 180
        ):
            return latitude, longitude
    except (TypeError, ValueError):
        pass

    metadata_text = "\n".join(
        str(row.get(field, ""))
        for field in ("name", "owner", "deployment_notes")
    )
    match = re.search(
        r"(?<![\d.])(\d{1,2}(?:\.\d+)?)\s*([NS])\s+"
        r"(\d{1,3}(?:\.\d+)?)\s*([EW])(?![A-Za-z])",
        metadata_text,
        re.IGNORECASE,
    )
    if not match:
        return None
    latitude = float(match.group(1)) * (-1 if match.group(2).upper() == "S" else 1)
    longitude = float(match.group(3)) * (-1 if match.group(4).upper() == "W" else 1)
    if not -90 <= latitude <= 90 or not -180 <= longitude <= 180:
        return None
    return latitude, longitude


def _metadata_with_coordinates(metadata_df):
    """Normalize usable station coordinates without mutating caller data."""
    if not isinstance(metadata_df, pd.DataFrame) or metadata_df.empty:
        return pd.DataFrame()
    rows = []
    for _, station in metadata_df.iterrows():
        coordinates = _coordinates_from_metadata(station)
        station_id = station.get("station_id")
        if coordinates is None or pd.isna(station_id):
            continue
        normalized = station.copy()
        normalized["station_id"] = str(station_id).strip()
        normalized["latitude"], normalized["longitude"] = coordinates
        rows.append(normalized)
    return pd.DataFrame(rows)


def fetch_single_buoy(station_id, preferred_data_source="none"):
    """Fetch one recent NDBC observation using the preferred source first."""
    station_id = str(station_id).strip()
    preferred = str(preferred_data_source or "none").strip().lower()
    parsers = (
        ("5day2", parse_5day2_data),
        ("realtime2", parse_realtime2_data),
    ) if preferred == "5day2" else (
        ("realtime2", parse_realtime2_data),
        ("5day2", parse_5day2_data),
    )
    for source, parser in parsers:
        observation = parser(station_id)
        if isinstance(observation, dict):
            observation = dict(observation)
            observation["source"] = source
            return pd.DataFrame([observation])
    return pd.DataFrame()


def create_placeholder_row(station, position_label, source="metadata_only"):
    """Keep nearby station context when no recent observation is available."""
    return {
        "station_id": str(station.get("station_id", "")),
        "station_name": station.get("name", "N/A"),
        "latitude": station.get("latitude"),
        "longitude": station.get("longitude"),
        "observation_time": pd.NaT,
        "temperature": np.nan,
        "wind_speed": np.nan,
        "wind_direction": np.nan,
        "wave_height": np.nan,
        "source": source,
        "timelate": np.nan,
        "distance_nm": station.get("distance_nm", np.nan),
        "position_label": position_label,
        "owner": station.get("owner", "Unknown"),
        "deployment_notes": station.get("deployment_notes", ""),
    }


def enrich_observation_with_metadata(station, observation, position_label):
    """Combine one provider observation with its station metadata."""
    observation_time = pd.to_datetime(
        observation.get("observation_time"), utc=True, errors="coerce"
    )
    timelate = observation.get("timelate")
    if pd.isna(timelate):
        timelate = calculate_timelate(observation_time)
    return {
        "station_id": str(station.get("station_id", "")),
        "station_name": station.get("name", "N/A"),
        "latitude": station.get("latitude"),
        "longitude": station.get("longitude"),
        "observation_time": observation_time,
        "temperature": observation.get("temperature"),
        "wind_speed": observation.get("wind_speed"),
        "wind_direction": observation.get("wind_direction"),
        "wave_height": observation.get("wave_height"),
        "source": observation.get("source", "NDBC"),
        "timelate": timelate,
        "distance_nm": station.get("distance_nm", np.nan),
        "position_label": position_label,
        "owner": station.get("owner", "Unknown"),
        "deployment_notes": station.get("deployment_notes", ""),
    }

def fetch_ndbc_buoy_data(lat, lon, position_label, max_buoys=10):
    try:
        lat, lon = float(lat), float(lon)
        if (
            not math.isfinite(lat)
            or not math.isfinite(lon)
            or not -90 <= lat <= 90
            or not -180 <= lon <= 180
        ):
            logging.warning("Invalid NDBC reference coordinates; returning no data")
            return pd.DataFrame()
        try:
            max_buoys = int(max_buoys)
        except (TypeError, ValueError, OverflowError):
            max_buoys = 10
        if max_buoys < 1:
            max_buoys = 10
        max_buoys = min(max_buoys, 100)

        metadata_df = _metadata_with_coordinates(load_buoy_metadata())
        if metadata_df.empty:
            logging.warning(
                "%s No buoy metadata with usable coordinates; returning empty DataFrame.",
                get_current_utc_timestamp(),
            )
            return pd.DataFrame()

        metadata_df['distance_nm'] = metadata_df.apply(
            lambda row: haversine_nm(lat, lon, row['latitude'], row['longitude']),
            axis=1
        )
        nearby_buoys = metadata_df.nsmallest(max_buoys, 'distance_nm').copy()

        if nearby_buoys.empty:
            logging.warning(f"{get_current_utc_timestamp()} âš ï¸ No nearby buoys found â€” returning empty DataFrame.")
            return pd.DataFrame()

        all_buoy_data = []
        for _, buoy in nearby_buoys.iterrows():
            buoy_data = fetch_single_buoy(buoy['station_id'], buoy.get('preferred_data_source', 'none'))

            if buoy_data.empty:
                all_buoy_data.append(create_placeholder_row(buoy, position_label, "metadata_only"))
            else:
                for _, obs in buoy_data.iterrows():
                    all_buoy_data.append(enrich_observation_with_metadata(buoy, obs, position_label))

        if all_buoy_data:
            df = pd.DataFrame(all_buoy_data)
        else:
            logging.warning(f"{get_current_utc_timestamp()} âš ï¸ No valid data from nearby buoys â€” returning empty DataFrame.")
            df = pd.DataFrame()

        logging.debug(f"âœ… Buoy DataFrame columns for Position {position_label}: {df.columns.tolist()}")
        logging.debug(f"âœ… Buoy DataFrame (first 5 rows) for Position {position_label}:\n{df.head()}")

        required_columns = ['latitude', 'longitude', 'station_id']
        for col in required_columns:
            if col not in df.columns:
                df[col] = np.nan

        return df

    except Exception as e:
        log_error_and_continue(f"{get_current_utc_timestamp()} âŒ Error fetching buoy data for Position {position_label}: {e}")
        return pd.DataFrame()

