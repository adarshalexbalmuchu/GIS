"""
LP dispatch model — assigns pumps to critical hotspots using PuLP.

Synchronous (CPU-bound). Only processes the top 30 hotspots per ward.
"""

from pulp import (
    LpMaximize,
    LpProblem,
    LpVariable,
    lpSum,
    value as lp_value,
)

MAX_PUMPS_PER_WARD = 15
MAX_PUMPS_PER_HOTSPOT = 3
TOP_N_HOTSPOTS = 30


def run_dispatch(
    ward_id: int,
    ward_name: str,
    ws_score: float,
    hotspots: list[dict],
) -> dict:
    """
    Solve a pump-assignment LP for *ward_id*.

    hotspots: list of dicts with keys
        {id, capacity_c, priority_weight, critical_penalty_pc}

    Returns a result dict suitable for dispatch_runs.result_json.
    """

    # ── Take top 30 by priority_weight ───────────────────────────────
    ranked = sorted(hotspots, key=lambda h: h["priority_weight"], reverse=True)
    ranked = ranked[:TOP_N_HOTSPOTS]

    if not ranked:
        return _empty_result(ward_id, ward_name, ws_score, "No hotspots to optimize")

    # ── Build LP ─────────────────────────────────────────────────────
    prob = LpProblem(f"dispatch_ward_{ward_id}", LpMaximize)

    x = {
        h["id"]: LpVariable(f"x_{h['id']}", lowBound=0, upBound=MAX_PUMPS_PER_HOTSPOT, cat="Integer")
        for h in ranked
    }

    # Objective: maximise weighted pump allocation
    prob += lpSum(h["priority_weight"] * x[h["id"]] for h in ranked)

    # Total pump cap
    prob += lpSum(x[h["id"]] for h in ranked) <= MAX_PUMPS_PER_WARD

    # Critical facilities must get at least 1 pump (if feasible)
    critical_count = sum(1 for h in ranked if h["critical_penalty_pc"] > 0)
    if critical_count <= MAX_PUMPS_PER_WARD:
        for h in ranked:
            if h["critical_penalty_pc"] > 0:
                prob += x[h["id"]] >= 1

    prob.solve()
    status = prob.status  # 1 = Optimal

    from pulp import constants as _c
    solver_status = _c.LpStatus.get(status, "Unknown")

    if solver_status != "Optimal":
        return _empty_result(
            ward_id, ward_name, ws_score,
            "Solver could not find optimal solution — manual review required.",
            solver_status=solver_status,
        )

    # ── Collect assignments ──────────────────────────────────────────
    assignments = []
    pumps_total = 0
    for h in ranked:
        pumps = int(lp_value(x[h["id"]]))
        if pumps >= 1:
            assignments.append({
                "hotspot_id": h["id"],
                "pumps": pumps,
                "priority": h["priority_weight"],
            })
            pumps_total += pumps

    n_locations = len(assignments)
    dispatch_message = (
        f"PMRS {ws_score:.1f} — Auto-dispatch: "
        f"{pumps_total} pumps to {n_locations} locations in {ward_name}"
    )

    return {
        "ward_id": ward_id,
        "ws_score": ws_score,
        "pumps_total": pumps_total,
        "assignments": assignments,
        "dispatch_message": dispatch_message,
        "solver_status": solver_status,
    }


# ── helper ───────────────────────────────────────────────────────────

def _empty_result(
    ward_id: int,
    ward_name: str,
    ws_score: float,
    message: str,
    solver_status: str = "N/A",
) -> dict:
    return {
        "ward_id": ward_id,
        "ws_score": ws_score,
        "pumps_total": 0,
        "assignments": [],
        "dispatch_message": message,
        "solver_status": solver_status,
    }
