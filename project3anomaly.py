import pandas as pd


# Data from https://github.com/IntelligentDDS/Nezha/blob/main/construct_data/2023-01-30/trace/11_39_trace.csv
df = pd.read_csv("project3data/train-ticket-traces.csv")


# Rename columns for clarity
df.rename(columns={
    'SpanID': 'span_id',
    'ParentID': 'parent_id',
    'PodName': 'service',
    'OperationName': 'operation',
    'StartTimeUnixNano': 'start_time',
    'EndTimeUnixNano': 'end_time'
}, inplace=True)

# Extract service name from pod name (everything before the first hyphen)
# df['service'] = df['pod_name'].str.replace(r'-[^-]+-[^-]+$', '', regex=True)

# Create lookup of spans by SpanID
span_lookup = df.set_index('span_id')

# Join spans with their parent spans
df['parent_service'] = df['parent_id'].map(span_lookup['service'])
df['parent_operation'] = df['parent_id'].map(span_lookup['operation'])
df['parent_start_time'] = df['parent_id'].map(span_lookup['start_time'])

# Filter: only inter-service calls (caller != callee)
inter_service_calls = df[df['service'] != df['parent_service']].copy()
# Filter: remove rows with missing caller_service (e.g., root spans)
inter_service_calls = inter_service_calls[inter_service_calls["parent_service"].notna()]

# Convert start time to readable datetime
inter_service_calls['time'] = pd.to_datetime(inter_service_calls['start_time'], unit='ns')
# Round start_time to nearest second (unix precision in seconds)
rounding_precision = 1_000_000_000    # 1s in nanoseconds
inter_service_calls['timestamp'] = (inter_service_calls['start_time'] // rounding_precision).astype(int)

# Calculate the interval number from the minimum start_time
interval_duration_ns = 1_000_000_000  # 1 second intervals
min_time_ns = inter_service_calls['start_time'].min()
inter_service_calls['interval_number'] = (
    (inter_service_calls['start_time'] - min_time_ns) // interval_duration_ns
).astype(int)

# Select relevant columns
result = inter_service_calls[[
    'parent_service',     # caller
    'service',            # callee
    'operation',
    'timestamp',
    'time',
    'interval_number'
]].rename(columns={
    'parent_service': 'caller_service',
    'service': 'callee_service'
})

# Optional: sort by time
result = result.sort_values(by='time')

result.to_csv("project3data/traces.csv", index=False, header=True)