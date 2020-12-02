import inspect
import os
import sys

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
top_dir = os.path.dirname(current_dir)

print(f"top_dir: {top_dir}")
if top_dir not in sys.path:
    sys.path.insert(0, top_dir)

from services.pv_simulator import main

if __name__ == "__main__":
    try:
        main.main(sys.argv)
    except Exception as ex:
        print(ex)
