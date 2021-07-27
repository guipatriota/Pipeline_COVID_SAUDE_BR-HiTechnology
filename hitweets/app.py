import os
import sys
sys.path.insert(0, os.path.abspath(os.path.realpath('.')))
sys.path.insert(1, os.path.abspath(os.path.dirname(os.path.realpath(__file__))))

from colect.colector import Colector
from transform.transformations import Transform
import time
import os
from datetime import datetime, timezone

def run_colector():
    try:
        start_time = time.perf_counter()
        batch_time_frame = 120#5*60
        transform_time = start_time + batch_time_frame
        colector = Colector()
        colector.run()
        run_transform = False
        while True:
            now_time = time.perf_counter()
            files_count = len([name for name in os.listdir('./colect/data')
                if os.path.isfile(os.path.join('./colect/data',name))])
            if transform_time < now_time and files_count > 0:
                run_transform = True
            if run_transform:
                transf = Transform()
                transf.run()
                run_transform = False
                transform_time = now_time + batch_time_frame
            
    except Exception as e: print(e)
    print('Stop execution.')
    time.sleep(30)
    run_colector()


if __name__ == "__main__":
    run_colector()