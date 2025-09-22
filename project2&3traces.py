import pandas as pd
import os


# Data from https://github.com/IntelligentDDS/Nezha/blob/main/construct_data/2023-01-30/trace/11_39_trace.csv
df = pd.read_csv(os.path.join("project2&3traces","train-ticket-traces.csv"))


# Rename columns for clarity
df.rename(columns={
    'SpanID': 'span_id',
    'ParentID': 'parent_id',
    'PodName': 'service',  # in this data, each service has a unique instance
    'OperationName': 'operation',
    'StartTimeUnixNano': 'start_time',
    'EndTimeUnixNano': 'end_time'
}, inplace=True)

# Create lookup of spans by SpanID
span_lookup = df.set_index('span_id')
# Identify the caller service by the parent_id of a callee service
df['caller_service'] = df['parent_id'].map(span_lookup['service'])

# Filter: only inter-service calls (callee != caller)
inter_service_calls = df[df['service'] != df['caller_service']].copy()
# Filter: remove rows with missing caller_service (i.e., root spans)
inter_service_calls = inter_service_calls[inter_service_calls["caller_service"].notna()]

# Rename to callee service
inter_service_calls.rename(columns={
    'service': 'callee_service'
}, inplace=True)

# Sort by time
inter_service_calls = inter_service_calls.sort_values(by='start_time')

# Project 2: keep all calls in real time (edgeflow network)
edgeflow = inter_service_calls[[
    'start_time'
    'caller_service',
    'callee_service',
    'operation']] #kept for reference, not used in the analysis

edgeflow.to_csv(os.path.join("project2&3traces", "project2edgeflow.csv"), index=False, header=True)

# Project 3: create network snapshots by grouping calls within intervals
snapshots = inter_service_calls[[
    'caller_service',
    'callee_service',
    'start_time']].copy()

# Aggregate network into intervals with given duration
# Note: this selection results in certain intervals being empty
# You should select your dataset and tune this value for your data
# so that you have consecutive intervals
interval_duration_ns = 1_000_000_000  # 1 second intervals

# Calculate the interval number from the minimum start_time
min_time_ns = snapshots['start_time'].min()
snapshots['interval'] = (
    (snapshots['start_time'] - min_time_ns) // interval_duration_ns
).astype(int)

# Keep only the interval
snapshots = snapshots[[
    'caller_service',
    'callee_service',
    'interval']]

# Count identical pairs caller->callee within each interval
snapshots = snapshots.value_counts().reset_index(name='count').sort_values('interval')

snapshots.to_csv(os.path.join("project2&3traces", "project3snapshots.csv"), index=False, header=True)
