"""
data_generation/manifest.py
=============================
Ghi corruption_manifest.json — audit log của các lỗi đã tiêm.

Mục đích:
  - Bằng chứng interview: chứng minh lỗi được tiêm có chủ đích và có kiểm soát
  - Đối chiếu với pipeline: dbt test và Speed Layer DLQ có bắt đúng số lỗi không?
  - Committed vào git tại docs/ — luôn có sẵn khi reviewer clone repo
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


def write(
    manifest_dev: dict,
    manifest_loadtest: dict,
    output_path: Path,
    dev_size: int,
    loadtest_size: int,
    random_seed: int,
) -> None:
    """
    Ghi manifest của cả 2 tập (dev + loadtest) vào 1 file JSON duy nhất.

    Args:
        manifest_dev:       Dict trả về từ corruption.inject() cho tập dev
        manifest_loadtest:  Dict trả về từ corruption.inject() cho tập loadtest
        output_path:        Path đến file output (docs/corruption_manifest.json)
        dev_size:           Số dòng dev trước khi tiêm lỗi
        loadtest_size:      Số dòng loadtest trước khi tiêm lỗi
        random_seed:        Seed đã dùng — để verify reproducibility
    """
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "random_seed": random_seed,
        "description": (
            "Audit log của các lỗi tiêm có chủ đích vào synthetic Berka data. "
            "Dùng để đối chiếu với kết quả dbt test và Speed Layer DLQ. "
            "Committed vào git tại docs/ làm bằng chứng phỏng vấn."
        ),
        "datasets": {
            "dev": {
                "target_rows": dev_size,
                **manifest_dev,
            },
            "loadtest": {
                "target_rows": loadtest_size,
                **manifest_loadtest,
            },
        },
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False, default=str)

    logger.info(f"Đã ghi corruption manifest → {output_path}")
    logger.info(
        f"  dev:      dup={manifest_dev['summary']['duplicate_count']:,}  "
        f"ref={manifest_dev['summary']['referential_violation_count']:,}  "
        f"missing={manifest_dev['summary']['missing_value_count']:,}"
    )
    logger.info(
        f"  loadtest: dup={manifest_loadtest['summary']['duplicate_count']:,}  "
        f"ref={manifest_loadtest['summary']['referential_violation_count']:,}  "
        f"missing={manifest_loadtest['summary']['missing_value_count']:,}"
    )