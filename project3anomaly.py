# Check project2&3traces.py on how the data is prepared

# Load trace snapshots
from tenetan.networks import SnapshotGraph
network = SnapshotGraph()
network.load_csv("project2&3traces/project3snapshots.csv", source="caller_service",
                 target="callee_service", weight="count", timestamp="interval",
                 sort_timestamps=False, sort_vertices=True)

print("Loaded temporal snapshot network of traces.")
# Number of nodes <-> number of services
print("Number of nodes:", network.N)
# Number of timestamps <-> number of non-empty intervals
print("Number of timestamps:", network.T)
# Number of edges -> should match the size of the csv
print("Number of edges:", network.E)

# Perform state detection using Graph Edit Distance (default)
# Last two returns are distance matrix (dm) and linkage matrix (lm, comes from scipy clustering)
from tenetan.state import MasudaHolme
best_C, labels, dunn_scores, dm, lm = MasudaHolme(network)

# labels[t, c-1] is the cluster (state) of interval t when using 'c' states;
# best_C is the index of the best clustering
best_labels = labels[:, best_C]

print("Graph Edit Distance - best number of states:", best_C+1)
print("Graph Edit Distance - best Dunn Index:", dunn_scores[best_C])

# Plot the states
import matplotlib.pyplot as plt
plt.figure()
# Our timestamps are interval numbers (integers)
xs = [int(t) for t in network.timestamps]
# Labels are originally 0-based ->
# Increase each cluster label by 1 to avoid plotting 0
ys = [l + 1 for l in best_labels]

# Plot using 'steps', i.e. horizontal lines until next interval
plt.step(xs, ys, where="post")

# Do not label the ticks on x-axis
# There are gaps since there were empty intervals during snapshot aggregation
l = [''] * len(xs)
plt.xticks(xs, l)

plt.title(f"States ({best_C+1}) based on Graph Edit Distance")
plt.tight_layout()
plt.show()


# Try another distance function, e.g. tenetan.static.distance.SpectralDistance
import tenetan
print("All distance functions:", tenetan.static.distance.__all__)

# Find documentation of SpectralDistance
print(help(tenetan.static.distance.SpectralDistance))

# We can modify e.g. the number of used eigenvectors
# MasudaHolme needs functions of form dist(A1, A2), which it will call internally
# We need to set other parameters of SpectralDistance using functools.partial
from functools import partial
n_eig = 2
my_dist = partial(tenetan.static.distance.SpectralDistance, n_eig=n_eig)

best_C, labels, dunn_scores, dm, lm =  MasudaHolme(network, dist=my_dist)
print(f"Spectral Distance ({n_eig=}) - best number of states:", best_C+1)
print(f"Spectral Distance ({n_eig=}) - best Dunn Index:", dunn_scores[best_C])

# Ignore best_C, assume we have 5 anomalies in the system -> we want to observe 5 states
n_states = 5
best_labels = labels[:, n_states-1]
print(f"Examining {n_states} states")

plt.figure()
# Our timestamps are interval numbers (integers)
xs = [int(t) for t in network.timestamps]
# Labels are originally 0-based ->
# Increase each cluster label by 1 to avoid plotting 0
ys = [l + 1 for l in best_labels]

# Plot using 'steps', i.e. horizontal lines until next interval
plt.step(xs, ys, where="post")

# Do not label the ticks on x-axis
# There are gaps since there were empty intervals during snapshot aggregation
l = [''] * len(xs)
plt.xticks(xs, l)

plt.title(f"States ({n_states}) based on Spectral Distance ({n_eig=})")
plt.tight_layout()
plt.show()

# Linkage matrix (lm) comes from SciPy, so we can plot it using its tools
from scipy.cluster.hierarchy import dendrogram
plt.figure()
dendrogram(lm)
plt.title("State detection dendrogram")
plt.show()