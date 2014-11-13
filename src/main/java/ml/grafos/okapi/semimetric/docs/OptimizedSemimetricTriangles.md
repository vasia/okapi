Remove Semi-metric Edges from Triangles (Optimized)
===================================================

## Overview
This is an optimized version of the `SemimetricTriangles` program.

## Details
This implementation divides the algorithm into several _megasteps_ which contain the three supersteps of the main computation. In each megastep, some of the vertices of the graph execute the algorithm, while the rest are idle (but still active). The algorithm finishes, when all vertices have executed the computation. In the case of semi-metric edge removal, this model is possible, because messages are neither aggregated nor combined. This implementation assumes numeric vertex IDs.

## How to Run

### Input Format
The input format is an edge list, containing one edge with its value per line, in the format `srcID\ttargetID\tweight` where the IDs are of type Long
and the weight is Double.

### Output Format
The output format is an edge list, containing one edge with its value per line, in the format `srcID\ttargetID\tweight` where the IDs are of type Long
and the weight is Double.

## Configuration Parameters
- semimetric.megasteps: Indicates in how many megasteps to divide the computation. The default value is 2.

## Running
`hadoop jar $OKAPI_JAR org.apache.giraph.GiraphRunner ml.grafos.okapi.graphs.ScalableSemimetric\$PropagateId -mc  ml.grafos.okapi.graphs.ScalableSemimetric\$SemimetricMasterCompute -eif ml.grafos.okapi.io.formats.LongDoubleTextEdgeInputFormat -eip $EDGES_INPUT -eof org.apache.giraph.io.formats.SrcIdDstIdEdgeValueTextOutputFormat -op $EDGE_OUTPUT -ca giraph.oneToAllMsgSending=true -ca giraph.outEdgesClass=org.apache.giraph.edge.HashMapEdges`

## References
- D. Ediger and D.A. Bader, Investigating Graph Algorithms in the BSP Model on the Cray XMT, 7th Workshop on Multithreaded Architectures and Applications (MTAAP), Boston, MA, May 24, 2013.

