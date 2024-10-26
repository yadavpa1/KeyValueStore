import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load the data without header (since the CSV doesn't have a header row)
load_data = pd.read_csv('data/load_balance.csv', header=None, names=['timestamp', 'cpu'])

# Assuming the data has 5 servers per group and 20 groups
servers_per_group = 5
num_groups = 20

# Add a 'Group' column to represent each group (1 to 20)
load_avg_data = pd.read_csv('data/load_balance_avg.csv', header=None, names=['pid', 'cpu_avg'])
load_avg_data['Group'] = [(i // servers_per_group) + 1 for i in range(len(load_avg_data))]

# Create a pivot table or reshape the data so that each row represents a group and each column represents a server
load_avg_pivot = load_avg_data.pivot_table(index='Group', columns=load_avg_data.index % servers_per_group, values='cpu_avg')
load_avg_pivot.columns = [f'Server {i+1}' for i in range(servers_per_group)]  # Rename columns

# 1. Heatmap of CPU utilization at the per-server level (5 columns x 20 rows)
plt.figure(figsize=(12, 8))
sns.heatmap(load_avg_pivot, cmap='YlGnBu', annot=True, fmt=".2f", cbar_kws={'label': 'Average CPU Utilization (%)'})
plt.title('CPU Utilization Heatmap per Server Across Groups')
plt.xlabel('Servers')
plt.ylabel('Groups')
plt.show()

# 2. Bar plot of average CPU load per group
group_avg = load_avg_pivot.mean(axis=1)

plt.figure(figsize=(10, 6))
plt.bar(group_avg.index, group_avg, color='skyblue')
plt.title('Average CPU Load per Group')
plt.xlabel('Group')
plt.ylabel('Average CPU Utilization (%)')
plt.xticks(range(1, num_groups + 1))  # Ensure xticks are integers
plt.grid(axis='y')
plt.show()

# 3. Line plot of standard deviation per group
group_std_dev = load_avg_pivot.std(axis=1)

plt.figure(figsize=(10, 6))
plt.plot(group_std_dev.index, group_std_dev, marker='o', linestyle='-', color='b')
plt.title('Standard Deviation of CPU Utilization per Group')
plt.xlabel('Group')
plt.ylabel('Standard Deviation (%)')
plt.xticks(range(1, num_groups + 1))  # Ensure xticks are integers
plt.grid(True)
plt.show()

# 4. Time series line plot for CPU utilization per group over time (with reduced xticks)
load_data_time = pd.read_csv('data/load_balance.csv')
load_data_time.set_index('timestamp', inplace=True)

# Assuming each server corresponds to columns from pid1, pid2, ..., pid100
grouped_avg_time = load_data_time.groupby((load_data_time.columns.to_series().str.extract(r'(\d+)', expand=False).astype(int) - 1) // servers_per_group, axis=1).mean()

plt.figure(figsize=(12, 6))
for group in range(num_groups):
    plt.plot(load_data_time.index, grouped_avg_time.iloc[:, group], label=f'Group {group + 1}')

# Reduce xticks for better readability
num_ticks = 10  # Number of x-ticks to show
xtick_labels = load_data_time.index[::len(load_data_time) // num_ticks]  # Select a subset of timestamps
plt.xticks(ticks=xtick_labels, labels=xtick_labels, rotation=45)

plt.title('Time Series of CPU Utilization per Group')
plt.xlabel('Time')
plt.ylabel('CPU Utilization (%)')
plt.legend(loc='upper right', fontsize='small')
plt.grid(True)
plt.show()
