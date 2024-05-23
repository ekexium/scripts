import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Set Seaborn style
sns.set(style="whitegrid")

# Read CSV file
df = pd.read_csv('test_results.csv')

# Ensure 'min flush keys' and 'max chunk size' columns are numeric
df['min flush keys'] = pd.to_numeric(df['min flush keys'])
df['max chunk size'] = pd.to_numeric(df['max chunk size'])

# Process data
df['label'] = df['min flush keys'].astype(str) + '-' + df['max chunk size'].astype(str) + '-bulk:' + df['bulk'].astype(str)
# Sort data
df = df.sort_values(by=['min flush keys', 'max chunk size', 'bulk'])

# Create pivot table
pivot_df = df.pivot_table(index=['SQL', 'label'], values=['latency', 'flush wait'], aggfunc='mean').reset_index()

# Get all SQL types and labels
sql_types = pivot_df['SQL'].unique()
labels = list(df['label'].unique())  # Keep labels order consistent with sorted data
x = np.arange(len(sql_types))  # Positions on the x-axis
width = 0.35  # Width of each group of bars
bar_width = width / len(labels)  # Width of each sub-bar

# Use Seaborn color palette
colors = sns.color_palette("Set2", len(labels))

# Plotting
fig, ax = plt.subplots(figsize=(14, 8))

# Plot each label's bars
for i, (label, color) in enumerate(zip(labels, colors)):
    sql_label_data = pivot_df[pivot_df['label'] == label]
    latency = sql_label_data['latency']
    flush_wait = sql_label_data['flush wait']
    
    # Plot overall latency bars
    bars = ax.bar(x + (i - len(labels) / 2) * bar_width, latency, bar_width, label=f'{label} latency', color=color)
    
    # Plot flush wait bars with transparent background and black grid lines
    for j, (bar, fw) in enumerate(zip(bars, flush_wait)):
        ax.bar(bar.get_x() + bar.get_width() / 2, fw, bar.get_width(), bottom=bar.get_y(), color=color, edgecolor='black', alpha=0.5, hatch='//', label='flush wait' if i == 0 and j == 0 else "")

# Set x-axis labels and title
ax.set_xlabel('SQL')
ax.set_ylabel('Time (ms)')
ax.set_title('Latency and Flush Wait by SQL Type and Parameters')
ax.set_xticks(x)
ax.set_xticklabels(sql_types)

# Adjust legend order, put flush wait at the end
handles, labels = ax.get_legend_handles_labels()
flush_wait_handle = handles.pop(labels.index('flush wait'))
flush_wait_label = labels.pop(labels.index('flush wait'))
handles.append(flush_wait_handle)
labels.append(flush_wait_label)

# Add description
handles.append(plt.Line2D([], [], color='none', label='min flush keys - max chunk size - bulk'))
labels.append('min flush keys - max chunk size - bulk')

# Set legend position
ax.legend(handles, labels, loc='upper left', bbox_to_anchor=(1, 1))

# Remove top and right border
sns.despine()

# Save plot to PNG file
plt.tight_layout()
plt.savefig('latency_flush_wait_by_sql_bulk.png', bbox_inches='tight')

# Show plot
plt.show()