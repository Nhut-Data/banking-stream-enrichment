"""
speed_layer/run.py
===================
Entrypoint cho Speed Layer.

CÁCH CHẠY:
  python -m speed_layer.run
  hoặc:
  make run-speed-layer
"""

from .consumer import run

if __name__ == "__main__":
    run()