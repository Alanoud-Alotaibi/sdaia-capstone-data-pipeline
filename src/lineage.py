"""
Deliverable 5 (cont.): Lineage tracking via OpenLineage.
Emit START/COMPLETE/FAIL events per pipeline stage.
"""

import json
from datetime import datetime
from contextlib import contextmanager
from src.config import LINEAGE_PATH

try:
    from openlineage.client.client import OpenLineageClient
    from openlineage.client.event_v1 import (
        RunEvent, RunState, Run, Job, Dataset, InputStateDef, OutputStateDef
    )
    HAS_OPENLINEAGE = True
except ImportError:
    HAS_OPENLINEAGE = False


def emit_lineage_event(stage: str, status: str, inputs: list = None, outputs: list = None):
    """
    Emit an OpenLineage event (START/COMPLETE/FAIL).
    For demo: write to JSON file instead of sending to Marquez.
    """
    event = {
        "timestamp": datetime.now().isoformat(),
        "stage": stage,
        "status": status,
        "inputs": inputs or [],
        "outputs": outputs or [],
        "job_id": f"sdaia-capstone-{stage}",
    }

    # Append to lineage file
    with open(LINEAGE_PATH, "a") as f:
        f.write(json.dumps(event) + "\n")

    print(f"📊 Lineage: {stage} -> {status}")


@contextmanager
def traced_stage(stage: str, inputs: list = None, outputs: list = None):
    """
    Context manager for tracing a pipeline stage.
    Emits START on enter, COMPLETE on success, FAIL on exception.
    """
    try:
        emit_lineage_event(stage, "START", inputs, outputs)
        yield
        emit_lineage_event(stage, "COMPLETE", inputs, outputs)
    except Exception as e:
        emit_lineage_event(stage, "FAIL", inputs, outputs)
        raise RuntimeError(f"Stage {stage} failed: {e}")
