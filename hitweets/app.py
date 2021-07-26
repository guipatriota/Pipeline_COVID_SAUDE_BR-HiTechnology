from colect.colector import Colector
from transform.transformations import Transform
import time
import os
from datetime import datetime, timezone

def run_colector():
    try:
        start_time = time.perf_counter()
        batch_time_frame = 60*5#5*60
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
                df = transf.run()
                timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
                archive_name = ('tw_covid_saude_'
                        + timestamp 
                        + '.json')
                run_transform = False
                transform_time = now_time + batch_time_frame
                datalake_folder = os.path.join(
                        os.path.dirname(os.path.realpath(__file__)),
                        'colect','data','datalake')
                df.to_json(os.path.join(
                        os.path.dirname(datalake_folder), archive_name),
                        orient='records')
                transf.clean_files()
            
    except Exception as e: print(e)
    print('Stop execution.')
    time.sleep(30)
    run_colector()


if __name__ == "__main__":
    run_colector()