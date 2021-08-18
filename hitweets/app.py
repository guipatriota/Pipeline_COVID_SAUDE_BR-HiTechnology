import os
import sys
import signal

sys.path.insert(0, os.path.abspath(os.path.realpath('.')))
sys.path.insert(1, os.path.abspath(
    os.path.dirname(os.path.realpath(__file__))))

from colect.colector import Colector
from transform.transformations import Transform
import time
import os
from datetime import datetime, timezone

def run():
    def signal_handler(signal, frame):
        sys.exit(0)

    try:
        signal.signal(signal.SIGINT, signal_handler)
        #start_time = time.perf_counter()
        #batch_time_frame = 25*60
        #transform_time = start_time + batch_time_frame
        colector = Colector()
        colector.run()
        run_transform = False
        while True:
            now_minute=datetime.now(timezone.utc).minute \
                        + datetime.now(timezone.utc).second/60
            #now_time = time.perf_counter()
            files_count = len([name for name in os.listdir(
                    os.path.join(os.path.dirname(
                        os.path.realpath(__file__)),'colect/data'))
                if os.path.isfile(
                    os.path.join(os.path.dirname(
                        os.path.realpath(__file__)),'colect/data',name))])
            #if transform_time < now_time and files_count > 0:
            print(now_minute)
            if (now_minute < 0.3 or now_minute < 30.3) and files_count > 0:
                run_transform = True

            if run_transform:
                transf = Transform()
                transf.run()
                run_transform = False
                #transform_time = now_time + batch_time_frame
            
    except Exception as e: print(e)
    print('Stop execution.')
    time.sleep(30)
    run()


if __name__ == "__main__":
    run()