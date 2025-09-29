import math
from pathlib import Path
import sys
import types

fake_pd = types.SimpleNamespace(
    isna=lambda value: isinstance(value, float) and math.isnan(value),
    DataFrame=object,
    Series=object,
    ExcelWriter=object,
)
sys.modules.setdefault("pandas", fake_pd)
fake_np = types.SimpleNamespace(nan=float("nan"))
sys.modules.setdefault("numpy", fake_np)

sys.path.append(str(Path(__file__).resolve().parents[1]))

from vendor.parser_tdnm.build_job_analytics import _workdays_per_month


def test_workdays_handles_nan_like_values():
    assert _workdays_per_month(None) is None
    assert _workdays_per_month(math.nan) is None
    assert _workdays_per_month(float("nan")) is None
    # Non-schedule numeric value should not crash and should return None
    assert _workdays_per_month(0) is None
