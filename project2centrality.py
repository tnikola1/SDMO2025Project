import csv
import os
import tenetan
from collections import defaultdict
import pandas as pd
import math

# Check project2&3data.py on how the data is prepared
# Load list of edges
EDGES = []
with open(os.path.join("project2&3traces", "project2edgeflow.csv"), 'r', newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        # Remove nanoseconds from timestamps to make integers smaller
        EDGES.append([row["caller_service"], row["callee_service"], int(row["start_time"])//1_000_000])

# Sort by time (redundant for data used in this script)
EDGES.sort(key = lambda x: x[2])

# Compute Temporal Katz Centrality with exponential decay using the quick method
beta=0.5
c=0.1
exp_centrality = tenetan.centrality.katz.LazyTemporalKatzCentrality(beta=beta, c=c)

# Helper function to compute all centralities
def get_temporal_centrality(centrality_func):
    # centrality[t][u] = centrality score of node u at time t
    centrality = defaultdict(dict)
    # keep track for which vertices centrality is already available
    seen_vertices = set()
    for (u, v, t) in EDGES:
        seen_vertices.add(u)
        seen_vertices.add(v)
        # Centrality is updated for v since edge is u -> v
        centrality[t][v] = centrality_func.update(u,v,t)
        # Get also centrality for the same t for all other nodes
        for node in seen_vertices - {v}:
            centrality[t][node] = centrality_func.query(node, t)

    # Create a table where columns are nodes and rows are timestamps
    df = pd.DataFrame.from_dict(centrality, orient="index")
    return df

# Compute centrality for each node at each time instance and save to csv
df = get_temporal_centrality(exp_centrality)
df.to_csv(os.path.join("project2&3traces", "project2_katz_exponential.csv"))

# To define own decay function, use general TemporalKatzCentrality class
# In this case, use constant decay function that returns 0.5
const_centrality = tenetan.centrality.katz.TemporalKatzCentrality(decay_function=lambda t: 0.5)
df = get_temporal_centrality(const_centrality)
df.to_csv(os.path.join("project2&3traces", "project2_katz_constant.csv"))

# Truncated centrality allows to consider only paths up to length K (in this case, K=2)
# In this case, also the function needs to be provided, e.g. same exponential
# (You can see that values deviate a bit from exponential centrality that kept track
# of all paths)
trunc_centrality = tenetan.centrality.katz.TruncatedTemporalKatzCentrality(decay_function=lambda t: beta*math.exp(-c*t), K=2)
df = get_temporal_centrality(trunc_centrality)
df.to_csv(os.path.join("project2&3traces", "project2_katz_truncated.csv"))