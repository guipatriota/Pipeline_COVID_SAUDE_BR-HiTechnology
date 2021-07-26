from colect.colector import Colector
from transform.transformations import Transform
import time

def run_colector():
    try:
        while True:
            start_time = time.perf_counter()
            run_transform = False
            transform_time = start_time
            colector = Colector()
            colector.run()
            if run_transform:
                transf = Transform()
                df = transf.run()
    except:
        print('Stop execution.')
        pass


if __name__ == "__main__":
    run_colector()