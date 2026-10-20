"""Opt-in diagnostic file output for local pipeline troubleshooting."""

import logging
import os
from pathlib import Path


LOG = logging.getLogger(__name__)


def dump_dataframe(dataframe, filename):
    """Write a diagnostic CSV only when ``RDS_DEBUG_DUMP=1`` is set.

    Alert data can contain operational details, and the web route may run many
    times. Keeping diagnostic output opt-in avoids overwriting files and
    persisting alert contents during normal operation.
    """
    if os.getenv("RDS_DEBUG_DUMP") != "1":
        return None

    output_dir = Path(os.getenv("RDS_DEBUG_DIR", "data/debugging"))
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename
    dataframe.to_csv(output_path, index=False)
    LOG.info("Wrote diagnostic DataFrame to %s", output_path)
    return output_path
