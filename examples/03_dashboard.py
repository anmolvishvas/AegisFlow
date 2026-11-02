"""Launch the AegisFlow Web Dashboard."""

import sys

import uvicorn

from aegisflow import create_dashboard_app

# Default to telemetry_pipeline.db or pass db path as an argument:
# python examples/03_dashboard.py ecommerce_saga.db
db_file = sys.argv[1] if len(sys.argv) > 1 else "telemetry_pipeline.db"

print(f"[INFO] Launching AegisFlow Web Dashboard on database: {db_file}")
print("[INFO] Open your browser and navigate to: http://localhost:8000")

app = create_dashboard_app(database_path=db_file)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
