import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# 设置Seaborn样式
sns.set(style="whitegrid")

# 读取CSV文件
df = pd.read_csv('test_results.csv')

# 确认 'min flush keys' 和 'max chunk size' 列是数字类型
df['min flush keys'] = pd.to_numeric(df['min flush keys'])
df['max chunk size'] = pd.to_numeric(df['max chunk size'])

# 处理数据
df['label'] = df['min flush keys'].astype(str) + '-' + df['max chunk size'].astype(str)
# 对数据进行排序
df = df.sort_values(by=['min flush keys', 'max chunk size'])

# 创建透视表
pivot_df = df.pivot_table(index=['SQL', 'label'], values=['latency', 'flush wait'], aggfunc='mean').reset_index()

# 获取所有SQL类型和标签
sql_types = pivot_df['SQL'].unique()
labels = list(df['label'].unique())  # 保持标签的顺序与排序后的数据一致
x = np.arange(len(sql_types))  # 横轴的位置
width = 0.35  # 每组柱子的宽度
bar_width = width / len(labels)  # 每个子柱子的宽度

# 使用Seaborn调色板
colors = sns.color_palette("Set2", len(labels))

# 绘图
fig, ax = plt.subplots(figsize=(14, 8))

# 绘制每个标签的柱状图
for i, (label, color) in enumerate(zip(labels, colors)):
    sql_label_data = pivot_df[pivot_df['label'] == label]
    latency = sql_label_data['latency']
    flush_wait = sql_label_data['flush wait']
    
    # 绘制整体 latency 部分
    bars = ax.bar(x + (i - len(labels) / 2) * bar_width, latency, bar_width, label=f'{label} latency', color=color)
    
    # 绘制 flush wait 部分，使用透明背景和黑色网格线
    for j, (bar, fw) in enumerate(zip(bars, flush_wait)):
        ax.bar(bar.get_x() + bar.get_width() / 2, fw, bar.get_width(), bottom=bar.get_y(), color=color, edgecolor='black', alpha=0.5, hatch='//', label='flush wait' if i == 0 and j == 0 else "")

# 设置横轴标签和标题
ax.set_xlabel('SQL')
ax.set_ylabel('Time (ms)')
ax.set_title('Latency and Flush Wait by SQL Type and Parameters')
ax.set_xticks(x)
ax.set_xticklabels(sql_types)

# 调整图例顺序， flush wait 放在最后
handles, labels = ax.get_legend_handles_labels()
flush_wait_handle = handles.pop(labels.index('flush wait'))
flush_wait_label = labels.pop(labels.index('flush wait'))
handles.append(flush_wait_handle)
labels.append(flush_wait_label)

# 添加说明
handles.append(plt.Line2D([], [], color='none', label='min flush keys - max chunk size'))
labels.append('min flush keys - max chunk size')

# 设置图例位置
ax.legend(handles, labels, loc='upper left', bbox_to_anchor=(1, 1))

# 去掉顶部和右侧的边框
sns.despine()

# 保存图表到PNG文件
plt.tight_layout()
plt.savefig('latency_flush_wait_by_sql.png', bbox_inches='tight')

# 显示图表
plt.show()