import tenetan

network = tenetan.networks.SnapshotGraph()
network.load_csv("project2&3traces/project3snapshots.csv", source="caller_service",
                 target="callee_service", weight="count", timestamp="interval",
                 sort_timestamps=True, sort_vertices=True)

print(network.N)
print(network.T)
print(network.vertices)
import numpy as np
print(np.count_nonzero(network.tensor))
