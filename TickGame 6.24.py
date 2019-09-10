import pandas as pd
import numpy as np
import datetime

fn = 'data/y_tick.csv'
df = pd.read_csv(fn)

extreme_points = df.loc[df.extreme != 0]

result = pd.DataFrame(columns=['trading_time', 'N_os/N_dc', 'Sigma_V_os/Sigma_V_dc'])

for i in range(len(extreme_points) - 1):
    # 分段操作，极值点之间为一段
    _df = df.loc[extreme_points.index[i]:extreme_points.index[i + 1]]
    _df['N_os/N_dc'] = np.nan
    _df['Sigma_V_os/Sigma_V_dc'] = np.nan

    _indices = _df.loc[_df.event != 0].index.values

    # 异常情况处理
    # 只有dc没有os
    if len(_indices) < 2:
        continue
    # 极值点有event值
    elif _indices[0] == _df.index.values[0]:
        if _df.loc[_indices[0], 'event'] == 1 or _df.loc[_indices[0], 'event'] == -1:
            if _df.loc[_indices[0], 'event'] == _df.loc[_indices[0], 'extreme']:
                # N_os/N_dc
                N_dc = _indices[1] - _df.index.values[0]
                N_os = _indices[2:] - _indices[1]
                _df.loc[_df.event.abs() == 2, 'N_os/N_dc'] = N_os / N_dc

                # Sigma_V_os/Sigma_V_dc
                Sigma_V_dc = _df.loc[:_indices[1], 'volume'].sum()
                Sigma_V_os = _df.loc[_indices[1]:_indices[-1], 'volume'].cumsum()[_indices[2:]]
                _df.loc[_df.event.abs() == 2, 'Sigma_V_os/Sigma_V_dc'] = Sigma_V_os / Sigma_V_dc

                _result = _df.loc[_df.event.abs() == 2, ['trading_time', 'N_os/N_dc', 'Sigma_V_os/Sigma_V_dc']]
                result = pd.concat([result, _result], axis=0)
                continue
            else:
                continue
        elif _df.loc[_indices[0], 'event'] == 2 or _df.loc[_indices[0], 'event'] == -2:
            # 只有dc和上一段最后的os两个点
            if len(_indices) < 3:
                continue
            else:
                if _df.loc[_indices[0], 'event'] == 2:
                    # N_os/N_dc
                    N_dc = _indices[1] - _df.index.values[0]
                    N_os = _indices[2:] - _indices[1]
                    _df.loc[_df.event.values == -2, 'N_os/N_dc'] = N_os / N_dc

                    # Sigma_V_os/Sigma_V_dc
                    Sigma_V_dc = _df.loc[:_indices[1], 'volume'].sum()
                    Sigma_V_os = _df.loc[_indices[1]:_indices[-1], 'volume'].cumsum()[_indices[2:]]
                    _df.loc[_df.event.values == -2, 'Sigma_V_os/Sigma_V_dc'] = Sigma_V_os / Sigma_V_dc

                    _result = _df.loc[_df.event.abs() == 2, ['trading_time', 'N_os/N_dc', 'Sigma_V_os/Sigma_V_dc']]
                    result = pd.concat([result, _result], axis=0)
                    continue
                elif _df.loc[_indices[0], 'event'] == -2:
                    # N_os/N_dc
                    N_dc = _indices[1] - _df.index.values[0]
                    N_os = _indices[2:] - _indices[1]
                    _df.loc[_df.event.values == 2, 'N_os/N_dc'] = N_os / N_dc

                    # Sigma_V_os/Sigma_V_dc
                    Sigma_V_dc = _df.loc[:_indices[1], 'volume'].sum()
                    Sigma_V_os = _df.loc[_indices[1]:_indices[-1], 'volume'].cumsum()[_indices[2:]]
                    _df.loc[_df.event.values == 2, 'Sigma_V_os/Sigma_V_dc'] = Sigma_V_os / Sigma_V_dc

                    _result = _df.loc[_df.event.abs() == 2, ['trading_time', 'N_os/N_dc', 'Sigma_V_os/Sigma_V_dc']]
                    result = pd.concat([result, _result], axis=0)
                    continue

    # N_os/N_dc
    N_dc = _indices[0] - _df.index.values[0]
    N_os = _indices[1:] - _indices[0]
    _df.loc[_df.event.abs() == 2, 'N_os/N_dc'] = N_os / N_dc

    # Sigma_V_os/Sigma_V_dc
    Sigma_V_dc = _df.loc[:_indices[0], 'volume'].sum()
    Sigma_V_os = _df.loc[_indices[0]:_indices[-1], 'volume'].cumsum()[_indices[1:]]
    _df.loc[_df.event.abs() == 2, 'Sigma_V_os/Sigma_V_dc'] = Sigma_V_os / Sigma_V_dc

    _result = _df.loc[_df.event.abs() == 2, ['trading_time', 'N_os/N_dc', 'Sigma_V_os/Sigma_V_dc']]
    result = pd.concat([result, _result], axis=0)

result.drop_duplicates('trading_time', inplace=True)

# 所有os点位及在原文件中index
os_df = df[(df['event'] == 2) | (df['event'] == -2)]
os_df.reset_index(inplace=True)

# 所有dc点位及在原文件中index
dc_df = df[(df['event'] == 1) | (df['event'] == -1)]
dc_df.reset_index(inplace=True)

# 所有extreme点位及在原文件中index
extreme_df = df[(df['extreme'] == 1) | (df['extreme'] == -1)]
extreme_df.reset_index(inplace=True)

# 找到每个os点位对应的start点
os_df.loc[:, 'start_index'] = 0

for i in range(len(os_df)):
    for j in range(len(dc_df) - 1):
        if dc_df.loc[j, 'index'] < os_df.loc[i, 'index'] < dc_df.loc[j + 1, 'index']:
            os_df.loc[i, 'start_index'] = dc_df.loc[j, 'index']

os_df.loc[:, 'start_index'] += 1

# 找到每个dc点位对应的start点
dc_df.loc[:, 'start_index'] = 0

for i in range(len(dc_df)):
    for j in range(len(extreme_df) - 1):
        if extreme_df.loc[j, 'index'] < dc_df.loc[i, 'index'] < extreme_df.loc[j + 1, 'index']:
            dc_df.loc[i, 'start_index'] = extreme_df.loc[j, 'index']

dc_df.loc[:, 'start_index'] += 1

# delta_t
ts = pd.to_datetime(df['trading_time'], format='%Y-%m-%d %H:%M:%S.%f')
delta_t = ts.diff()
df['delta_t'] = delta_t
# delta_t_os
for i in range(len(os_df)):
    os_df.loc[i, 'delta_t_os'] = df.loc[os_df.loc[i, 'start_index']:1 + os_df.loc[i, 'index'], 'delta_t'].where(
        df['delta_t'] <= datetime.timedelta(0, 60)).sum()
# delta_t_dc
for i in range(len(dc_df)):
    dc_df.loc[i, 'delta_t_dc'] = df.loc[dc_df.loc[i, 'start_index']:1 + dc_df.loc[i, 'index'], 'delta_t'].where(
        df['delta_t'] <= datetime.timedelta(0, 60)).sum()
# delta_t_os/delta_t_dc
for i in range(len(os_df)):
    for j in range(len(dc_df)):
        if os_df.loc[i, 'start_index'] - 1 == dc_df.loc[j, 'index']:
            os_df.loc[i, 'delta_t_os/delta_t_dc'] = os_df.loc[i, 'delta_t_os'] / dc_df.loc[j, 'delta_t_dc']

out_df = os_df.merge(result, on='trading_time')
out_df = out_df[['index', 'start_index', 'trading_time', 'N_os/N_dc', 'delta_t_os/delta_t_dc',
                 'Sigma_V_os/Sigma_V_dc']]

# os和dc的开始结束时间
for i in range(len(out_df)):
    out_df.loc[i, 'os_start_time'] = df.loc[out_df.loc[i, 'start_index'], 'trading_time']
    out_df.loc[i, 'os_end_time'] = df.loc[out_df.loc[i, 'index'], 'trading_time']
    for j in range(len(dc_df)):
        if os_df.loc[i, 'start_index'] - 1 == dc_df.loc[j, 'index']:
            out_df.loc[i, 'dc_start_time'] = df.loc[dc_df.loc[j, 'start_index'], 'trading_time']
            out_df.loc[i, 'dc_end_time'] = df.loc[dc_df.loc[j, 'index'], 'trading_time']

# 最终out_df
out_df = out_df[['os_start_time', 'os_end_time', 'dc_start_time', 'dc_end_time', 'N_os/N_dc', 'delta_t_os/delta_t_dc',
                 'Sigma_V_os/Sigma_V_dc']]

out_file = 'output/result.csv'
out_df.to_csv(out_file, index=False)
