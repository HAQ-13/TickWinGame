import pandas as pd
import numpy as np
import datetime
import time
time_start = time.time()

prod = 'y'
fn = f'./data/{prod}_tick.csv'
df = pd.read_csv(fn)

ts = pd.to_datetime(df['trading_time'], format='%Y-%m-%d %H:%M:%S.%f')
df['delta_t'] = ts.diff()

extreme_points = df.loc[df.extreme != 0]
result = pd.DataFrame(columns=['trading_time', 'event', 'N_os/N_dc', 'Delta_t_os/Delta_t_dc', 'Sigma_V_os/Sigma_V_dc'])

for i in range(len(extreme_points) - 1):
    # 分段操作，极值点之间为一段
    _df = df.loc[extreme_points.index[i]:extreme_points.index[i + 1]]
    _df['N_os/N_dc'] = np.nan
    _df['Sigma_V_os/Sigma_V_dc'] = np.nan
    _df['Delta_t_os/Delta_t_dc'] = np.nan

    _indices = _df.loc[_df.event != 0].index.values
    start_index = _df.index.values[0]
    dc_index = _indices[0]
    os_index = _indices[1:]

    # 特殊情况处理
    # 只有dc没有os
    if len(_indices) < 2:
        continue
    # 分段起始点(极值点)有event
    elif dc_index == start_index:
        if len(_indices) < 3:
            continue
        else:
            dc_index = _indices[1]
            os_index = _indices[2:]

    # N_os/N_dc
    N_dc = dc_index - start_index
    N_os = os_index - dc_index
    _df.loc[os_index, 'N_os/N_dc'] = N_os / N_dc

    # Sigma_V_os/Sigma_V_dc
    Sigma_V_dc = _df.loc[start_index + 1 : dc_index, 'volume'].sum()
    Sigma_V_os = _df.loc[dc_index + 1 : os_index[-1], 'volume'].cumsum()[os_index]
    _df.loc[os_index, 'Sigma_V_os/Sigma_V_dc'] = Sigma_V_os / Sigma_V_dc

    # Delta_t_os/Delta_t_dc
    delta_t_dc = _df.loc[start_index + 1 : dc_index, 'delta_t'].loc[
        _df['delta_t'] <= datetime.timedelta(seconds=59)].sum()
    # 将超时值置为500ms，即tick的delta_t理论值，以防os_index行被过滤掉
    _df.loc[dc_index + 1 : os_index[-1], 'delta_t'].loc[
        _df['delta_t'] > datetime.timedelta(seconds=59)] = pd.Timedelta(500, unit='ms')
    delta_t_os = _df.loc[dc_index + 1 : os_index[-1], 'delta_t'].cumsum()[os_index]
    _df.loc[os_index, 'Delta_t_os/Delta_t_dc'] = delta_t_os / delta_t_dc

    _result = _df.loc[os_index, ['trading_time', 'event', 'N_os/N_dc', 'Delta_t_os/Delta_t_dc', 'Sigma_V_os/Sigma_V_dc']]
    result = pd.concat([result, _result], axis=0)

print(time.time() - time_start)
result.to_csv(f'./data/{prod}_scaling_law_features.csv', index=False)