import os
import sys
from array import array
from collections import defaultdict
from datetime import datetime, timedelta

import matplotlib.pyplot as plt
import numpy as np
import zstd
from botocore.exceptions import ClientError
from tqdm import tqdm

sys.path.append(os.path.join("/".join(os.getcwd().split("/")), "src"))
import pickle

from DataLoader import DataLoader


def load_data(dl, config):
    query_results = dl.query(
        site=config["site"],
        process=config["process"],
        line=config["line"],
        equipment=config["equipment"],
        number=config["number"],
        start_time=config["start_time"],
        end_time=config["end_time"],
    )

    data_dict = defaultdict(dict)

    for result in tqdm(query_results):
        try:
            real_state = result[2][:27]
            phase = result[2][-5:-4]

            raw_data = dl.load(result)
            bytes_data = zstd.decompress(raw_data)
            float_data = np.array(array("f", bytes_data))

            data_dict[real_state][phase] = float_data[:]

        except ClientError as e:
            if e.response["Error"]["Code"] == "InternalError":
                continue
            else:
                print("An error occurred:", e)
    return data_dict

if __name__ == "__main__":
    dl = DataLoader()
    # problem = "2023-07-15 16:11:00"
    # problem_datetime = datetime.strptime(problem, "%Y-%m-%d %H:%M:%S")
    # start_time = problem_datetime - timedelta(days=90)
    # end_time = problem_datetime + timedelta(days=90)

    config = {
        "site": "eswa",
        "process": "lam",
        "line": "15",
        "equipment": "01",
        "number": 4,
        "start_time": "2023-04-15 00:00:00",
        "end_time": "2023-07-11 00:00:00",
        "data_shift_ind": 60,
    }

    data_dict = load_data(dl, config)
    
    with open('15_1_4_data_dict.pkl', 'wb') as f:
        pickle.dump(data_dict, f)