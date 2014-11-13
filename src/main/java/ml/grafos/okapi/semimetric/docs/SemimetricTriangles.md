Remove Semi-metric Edges from Triangles
=======================================

## Overview
In a weighted graph, we say that a direct edge between two nodes is _semi-metric_, if there exists an indirect path between these two nodes, with a shorter distance.
This program takes as input an undirected, weighted graph and removes all 1st-order semi-metric edges from it; i.e. all semi-metric edges, for which the shorter indirect path has length two. In order to find all 1st-order semimetric edges, we first find all the triangles in the graph and then compare their weights. If vertices A, B, C form a triangle, then edge AB is semi-metric if `D(A,B) > D(A,C)+D(C,B)`, where `D(u, v)` is the weight of the edge `(u, v)`.

## Details
The algorithm consists of four supersteps and discovers each triangle exactly once. We assume mnumeric vertex IDs and a total ordering on them. 
In the first superstep, every node propagates its ID to all its neighbors with higher ID. In the second superstep, each node retrieves the
ID from each received message and the weight of the edge that is formed between this node and the message sender. Then, the node constructs a new message with
the sender ID, the edge weight and its own ID and sends this message to all the neighbors with higher ID. In the third superstep, for every message received,
a node checks if a triangle is formed and, in case it is, whether it contains a semi-metric edge. If the node detects a semi-metric edge, it marks this and the
opposite direction edge for removal. In the final superstep, all marked edges are removed.

## How to Run

### Input Format
The input format is an edge list, containing one edge with its value per line, in the format `srcID\ttargetID\tweight` where the IDs are of type Long
and the weight is Double.

### Output Format
The output format is an edge list, containing one edge with its value per line, in the format `srcID\ttargetID\tweight` where the IDs are of type Long
and the weight is Double.

## Configuration Parameters
- semimetric.remove.edges.enabled: When set to `true`, the encountered semi-metric edges will be removed from the graph. This parameter is set to `true` by default.

## Running
`hadoop jar $OKAPI_JAR org.apache.giraph.GiraphRunner ml.grafos.okapi.graphs.SemimetricTriangles\$PropagateId -mc  ml.grafos.okapi.graphs.SemimetricTriangles\$SemimetricMasterCompute -eif ml.grafos.okapi.io.formats.LongDoubleTextEdgeInputFormat -eip $EDGES_INPUT -eof org.apache.giraph.io.formats.SrcIdDstIdEdgeValueTextOutputFormat -op $EDGE_OUTPUT -ca giraph.oneToAllMsgSending=true -ca giraph.outEdgesClass=org.apache.giraph.edge.HashMapEdges`

## References
- D. Ediger and D.A. Bader, Investigating Graph Algorithms in the BSP Model on the Cray XMT, 7th Workshop on Multithreaded Architectures and Applications (MTAAP), Boston, MA, May 24, 2013.

