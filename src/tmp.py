from src.data_loader import DataLoader
import numpy as np
import zstd

dl = DataLoader()
cctx = zstd.ZstdCompressor()

config = {
    'site': "eswa",
    'process': "lam",
    'line': 13,
    'equipment': '01',
    'number': 1,
    'start_time': "2023-01-01 00:00:00",
    'end_time': "2023-11-30 10:00:00",
    'data_shift_ind': 60,
}


query_results = dl.query(
                site=config['site'],
                process=config['process'],
                line = config['line'],
                equipment = config['equipment'],
                number= config['number'],
                start_time=config['start_time'],
                end_time=config['end_time'],
            )

len(query_results)

motor_info = f"{int(config['line']):02d}_{int(config['equipment']):02d}_{int(config['number']):02d}"
query_date_list = np.unique([query[2].split('_')[0] for query in query_results])
print(len(query_date_list))



def get_data_ray(key,):
    raw_data = dl.load((config['site'], config['process'], key+'_'+"u"+'.zst'))
    decompressed = zstd.decompress(raw_data)
    current_data = np.frombuffer(decompressed, dtype=np.float32)
    return current_data


%%time
MAX_NUM_PENDING_TASKS = 30
NUM_RETURNS = 15
##########
result_refs = []
result_list = []
data_list = []
sub_key_list = []
for key in tqdm(query_date_list[::config['data_shift_ind']]):
    sub_key_list.append(key)
    if len(result_refs) > MAX_NUM_PENDING_TASKS:
        ready_refs, result_refs = ray.wait(result_refs, num_returns=NUM_RETURNS)
        sub_results = ray.get(ready_refs)
        result_list.extend(sub_results)
        del ready_refs, sub_results
        gc.collect()
    result_refs.append(get_data_ray.remote(key))
sub_results = ray.get(result_refs)
result_list.extend(sub_results)
data_df = pd.DataFrame()
data_df.loc[:,'key'] = sub_key_list
data_df.loc[:,'u_signal'] = result_list
data_df = data_df.loc[np.array([len(signal) for signal in data_df.u_signal.values]) == 400000]
data_df.index = range(len(data_df))
data_df.head()