import pandas as pd
import datetime

# 文件位置
file1 = 'data/y_tick.csv'
file2 = 'data/y_tb_label.csv'

# 读取文件
in1_df = pd.read_csv(file1)
in2_df = pd.read_csv(file2)

# 所有os点位及在原文件中index
os_df = in1_df[(in1_df['event'] == 2) | (in1_df['event'] == -2)]
os_df.reset_index(inplace=True)

# 所有dc点位及在原文件中index
dc_df = in1_df[(in1_df['event'] == 1) | (in1_df['event'] == -1)]
dc_df.reset_index(inplace=True)

# 所有extreme点位及在原文件中index
extreme_df = in1_df[(in1_df['extreme'] == 1) | (in1_df['extreme'] == -1)]
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

# N_os
os_df['N_os'] = os_df['index'] - (os_df['start_index'] - 1)
# N_dc
dc_df['N_dc'] = dc_df['index'] - (dc_df['start_index'] - 1)
# N_os/N_dc
for i in range(len(os_df)):
    for j in range(len(dc_df)):
        if os_df.loc[i, 'start_index'] - 1 == dc_df.loc[j, 'index']:
            os_df.loc[i, 'N_os/N_dc'] = os_df.loc[i, 'N_os'] / dc_df.loc[j, 'N_dc']

# delta_t
ts = pd.to_datetime(in1_df['trading_time'], format='%Y-%m-%d %H:%M:%S.%f')
delta_t = ts.diff()
in1_df['delta_t'] = delta_t

# delta_t_os
for i in range(len(os_df)):
    os_df.loc[i, 'delta_t_os'] = in1_df.loc[os_df.loc[i, 'start_index']:1 + os_df.loc[i, 'index'], 'delta_t'].where(
        in1_df['delta_t'] <= datetime.timedelta(0, 60)).sum()
# delta_t_dc
for i in range(len(dc_df)):
    dc_df.loc[i, 'delta_t_dc'] = in1_df.loc[dc_df.loc[i, 'start_index']:1 + dc_df.loc[i, 'index'], 'delta_t'].where(
        in1_df['delta_t'] <= datetime.timedelta(0, 60)).sum()
# delta_t_os/delta_t_dc
for i in range(len(os_df)):
    for j in range(len(dc_df)):
        if os_df.loc[i, 'start_index'] - 1 == dc_df.loc[j, 'index']:
            os_df.loc[i, 'delta_t_os/delta_t_dc'] = os_df.loc[i, 'delta_t_os'] / dc_df.loc[j, 'delta_t_dc']

# sigma_v_os
for i in range(len(os_df)):
    os_df.loc[i, 'sigma_v_os'] = in1_df.loc[os_df.loc[i, 'start_index']:1 + os_df.loc[i, 'index'], 'volume'].sum()
# sigma_v_dc
for i in range(len(dc_df)):
    dc_df.loc[i, 'sigma_v_dc'] = in1_df.loc[dc_df.loc[i, 'start_index']:1 + dc_df.loc[i, 'index'], 'volume'].sum()
# sigma_v_os/sigma_v_dc
for i in range(len(os_df)):
    for j in range(len(dc_df)):
        if os_df.loc[i, 'start_index'] - 1 == dc_df.loc[j, 'index']:
            os_df.loc[i, 'sigma_v_os/sigma_v_dc'] = os_df.loc[i, 'sigma_v_os'] / dc_df.loc[j, 'sigma_v_dc']

# out_df
out_df = os_df.copy()
out_df = out_df[['index', 'start_index', 'trade_day', 'trading_time', 'N_os/N_dc', 'delta_t_os/delta_t_dc',
                 'sigma_v_os/sigma_v_dc']]

# os和dc的开始结束时间
for i in range(len(out_df)):
    out_df.loc[i, 'os_start_time'] = in1_df.loc[out_df.loc[i, 'start_index'], 'trading_time']
    out_df.loc[i, 'os_end_time'] = in1_df.loc[out_df.loc[i, 'index'], 'trading_time']
    for j in range(len(dc_df)):
        if os_df.loc[i, 'start_index'] - 1 == dc_df.loc[j, 'index']:
            out_df.loc[i, 'dc_start_time'] = in1_df.loc[dc_df.loc[j, 'start_index'], 'trading_time']
            out_df.loc[i, 'dc_end_time'] = in1_df.loc[dc_df.loc[j, 'index'], 'trading_time']

# 最终out_df
out_df = out_df[['os_start_time', 'os_end_time', 'dc_start_time', 'dc_end_time', 'N_os/N_dc', 'delta_t_os/delta_t_dc',
                 'sigma_v_os/sigma_v_dc']]

out1_file = 'output/result.csv'
out_df.to_csv(out1_file, index=False)
