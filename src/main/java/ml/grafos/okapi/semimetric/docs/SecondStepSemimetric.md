Identification of Metric Edges
===============================

## Overview
This program takes as input a graph with no 1st-order semi-metric edges. Each node exploits information in its two-hop neighborhood to reason about the semi-metricity
of its edges. The goal is to detect as many _metric_ edges as possible, by looking at the two-hop neighborhood. First, each node marks its lowest-weight edges as metric, and then checks whether it can reason about the semi-metricity of the rest of its edges, by comparing their weights to the minimum weights of its two-hop paths, which contain metric edges.

## Details
The program consists of an initialization superstep and two main supersteps, which are iteratively executed, until no further metric edges can be found. During the initialization superstep, each vertex retrieves its edges that have the lowest weight and marks them as metric. Then, it sends a message with its ID and the edge weight, along the identified metric edges. In the second superstep, the vertices that receive a message are target nodes of already identified metric edges. These vertices use this information to mark as metric the opposite direction edges. Then, every vertex sends one message along all its metric edges. For each metric edge, the message contains the sum of the weight  of this edge, plus the smallest among the weights of the rest of this node's edges. In other words, each vertex sends along each metric edge, a message with the distance of the shortest two-hop path, that passes through this vertex and contains this edge. In the third step, each vertex checks whether it can reason about the semi-metricity of its smallest-weight unlabeled edge. If all of the weights in the received messages are larger than the weight of this edge, then this edge can be safely marked as metric. If an edge is marked as metric, then the node sends a message to its target vertex, so that the opposite direction edge can be also marked as metric.

## How to Run

### Input Format
The input format is an edge list, containing one edge with its value per line, in the format `srcID\ttargetID\tweight` where the IDs are of type Long
and the weight is Double.

### Output Format
The output format is an edge list, containing one edge with its weight and a label per line, in the format `srcID\ttargetID\tweightt\isMetric` where the IDs are of type Long, the weight is Double and the label is a boolean. A label with value `true` means that the edge is metric.

## Configuration Parameters
- semimetric.megasteps: Indicates in how many megasteps to divide the computation. The default value is 2.

## Running
`hadoop jar $OKAPI_JAR org.apache.giraph.GiraphRunner ml.grafos.okapi.graphs.ScalableSemimetric\$PropagateId -mc  ml.grafos.okapi.graphs.ScalableSemimetric\$SemimetricMasterCompute -eif ml.grafos.okapi.io.formats.LongDoubleTextEdgeInputFormat -eip $EDGES_INPUT -eof org.apache.giraph.io.formats.SrcIdDstIdEdgeValueTextOutputFormat -op $EDGE_OUTPUT -ca giraph.oneToAllMsgSending=true -ca giraph.outEdgesClass=org.apache.giraph.edge.HashMapEdges`

## References
