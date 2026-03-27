"""Run 2023 backtest and print results."""
import httpx
import json
import time

BASE = "http://localhost:8000"
HEADERS = {"x-api-key": "hydro-mvp-secret-2026"}

# Wait for backend to be ready
for i in range(30):
    try:
        r = httpx.get(f"{BASE}/map/state", timeout=5)
        if r.status_code == 200:
            print("Backend up")
            break
    except Exception:
        pass
    time.sleep(3)
else:
    print("Backend not ready after 90s")
    exit(1)

# Trigger backtest
print("Starting 2023 backtest...")
r = httpx.post(f"{BASE}/backtest/2023", headers=HEADERS, timeout=10)
print(f"POST status: {r.status_code}")

# Poll for result
for i in range(120):
    time.sleep(5)
    try:
        r = httpx.get(f"{BASE}/backtest/2023/status", timeout=10)
        status = r.json()
        st = status.get("status")
        msg = status.get("message", "")
        print(f"  Poll {i+1}: {st}, {msg}")
        if st == "complete":
            break
        if st == "error":
            print(f"  ERROR: {status}")
            break
    except Exception as e:
        print(f"  Poll error: {e}")

# Get result
r = httpx.get(f"{BASE}/backtest/2023/result", timeout=10)
result = r.json()
print()
print(f"Precision: {result.get('precision_pct')}%")
print(f"Recall: {result.get('recall_pct')}%")
print(f"F1: {result.get('f1_pct')}%")
cm = result.get("confusion_matrix", {})
print(f"TP: {cm.get('true_positives')}, FN: {cm.get('false_negatives')}, FP: {cm.get('false_positives')}, TN: {cm.get('true_negatives')}")
print(f"Ref flooded: {result.get('reference_flooded_count')}, Pred flooded: {result.get('predicted_flooded_count')}")

# Show events
events = result.get("events", [])
for ev in events:
    print(f"  {ev.get('date')}: rain={ev.get('rain_mm')}mm yamuna={ev.get('yamuna_m')}m triggered={ev.get('wards_triggered')} critical={ev.get('wards_critical')}")
