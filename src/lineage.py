"""
Deliverable 5 (cont.): Lineage tracking via OpenLineage standard events.
Emits START, COMPLETE, and FAIL events for every pipeline stage.
"""

import json
import uuid
from datetime import datetime, timezone
from contextlib import contextmanager
from typing import List, Optional

try:
    from openlineage.client.event_v1 import (
        Dataset
    )
    HAS_OPENLINEAGE = True
except ImportError:
    HAS_OPENLINEAGE = False

from src.config import LINEAGE_PATH


def emit_lineage_event(
    stage_name: str,
    event_type: str,
    inputs: Optional[List[str]] = None,
    outputs: Optional[List[str]] = None,
    run_id: Optional[str] = None
) -> dict:
    """
    Emit an OpenLineage specification compliant event (START, COMPLETE, FAIL).
    """
    current_run_id = run_id or str(uuid.uuid4())
    event_time = datetime.now(timezone.utc).isoformat()

    input_datasets = [{"namespace": "sdaia-capstone", "name": inp} for inp in (inputs or [])]
    output_datasets = [{"namespace": "sdaia-capstone", "name": out} for out in (outputs or [])]

    state_map = {
        "START": "START",
        "COMPLETE": "COMPLETE",
        "FAIL": "FAIL"
    }
    event_state = state_map.get(event_type.upper(), "OTHER")

    event_payload = {
        "eventType": event_state,
        "eventTime": event_time,
        "run": {"runId": current_run_id},
        "job": {
            "namespace": "sdaia-capstone-pipeline",
            "name": f"job_{stage_name}"
        },
        "inputs": input_datasets,
        "outputs": output_datasets,
        "producer": "https://github.com/SDAIAAcademy/sdaia-capstone-data-pipeline"
    }

    with open(LINEAGE_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(event_payload) + "\n")

    print(f"[LINEAGE] [{event_state}] Event Emitted: Stage='{stage_name}' | RunID='{current_run_id[:8]}'")
    return event_payload


@contextmanager
def traced_stage(stage_name: str, inputs: Optional[List[str]] = None, outputs: Optional[List[str]] = None):
    """
    Context manager for OpenLineage stage tracing.
    Emits START upon entry, COMPLETE upon success, and FAIL upon exception.
    """
    run_id = str(uuid.uuid4())
    emit_lineage_event(stage_name, "START", inputs, outputs, run_id=run_id)
    try:
        yield
        emit_lineage_event(stage_name, "COMPLETE", inputs, outputs, run_id=run_id)
    except Exception as e:
        emit_lineage_event(stage_name, "FAIL", inputs, outputs, run_id=run_id)
        raise e
