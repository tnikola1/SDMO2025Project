# 811372A-3007 Software Development, Maintenance and Operations 2025 Projects

This repository contains example data and scripts showcasing data collection and processing
for projects of the Software Development, Maintenance and Operations.

The three projects are:

- Project 1: Developer de-duplication
- Project 2: Temporal centrality and monitoring metrics
- Project 3: Temporal state detection and anomaly detection

## Contents

- `project1devs/`: Directory with data for Project 1
  - `devs.csv`: List of developers mined from eShopOnContainersProject
  - `devs_similarity.csv`: Similarity tests for each pair of developers
  - `devs_similarity_t=0.7.csv`: Similarity tests for each pair of developers with similarity threshold 0.7
- `project1developers.py`: Script demonstrating mining developer information and Bird heuristic to determine duplicate developers
- `project2&3traces/`: Directory with data for Projects 2 & 3
  - `train-ticket-traces.csv`: CSV file with traces for train-ticket system from an open dataset
  - `project2edgeflow.csv`: CSV file providing a list of real-time service calls for Project 2
  - `project3anomaly.csv`: CSV file providing snapshot networks for service calls grouped by intervals for Project 3
- `project2&3data.py`: Script processing raw traces into formats required by Projects 2&3
- `project3anomaly.py`: Script demonstrating temporal state detection on temporal networks of microservice calls for Project 3
