Adamic-Adar
===========

## Overview
Like Jaccard, the Adamic-Adar similarity also represents how similar two nodes of a graph are, based on their common neighbors. However, it extends the simple counting of common neighbors, by weighting the similarity value according to the nodes' degrees.
In particular, it is computed as the sum of the log of the inverse degree of each common neighbor of the two nodes. The negative value of the metric computed gives a distance measure, with values in the range [0, INF]. This implementation computes the Adamic-Adar index only for existing edges of the input graph and not for every pair of nodes.

## Details
The computation consists of 3 supersteps. During the first superstep, each vertex computes the logarithm of its inverse degree and sets this value as its vertex value.
In the second superstep, each vertex sends a list of its friends and its vertex value to all its neighbors in the graph.
In the third superstep, each vertex computes the Adamic-Adar index value for each of its edges. 
If the configuration option for distance conversion is enabled, then an additional superstep is executed, where the Adamic-Adar index is converted to a distance index, using the function, by negating the value.
If the input graph is large, the computation of the Adamic-Adar index for all the edges might be very expensive. For this reason, an approximate implementation is also provided. In this implementation, each vertex uses a BloomFilter to store its neighborhood and sends this filter as a message to its neighbors, instead of the exact friend list. The hash functions to use and the size of the BloomFilter are configurable. 

## How to Run

### Input Format
The input format is an edge list, containing one edge per line, in the format `srcID\ttargetID` where the IDs are of type Long.

### Output Format
The output of this program is the edges of the graph with their Adamic-Adar index, one edge per line, in the form:
		srcID\ttargetID\tindex
where the IDs are of type Long and similarity is Double.

## Configuration Parameters
- adamicadar.approximation.enabled: When set to `true`, the bloom filter implementation will be used and the program will compute the approximate Adamic-Adar index. The deafult value is `false`.
- adamicadar.bloom.filter.bits: Size of the bloom filter, in bits. The default value if 16.
- adamicadar.bloom.filter.functions: The number of hash functions to use for the bloom filter. The default value is 1.
- adamicadar.bloom.filter.hash.type: The type of the hash functions to use for the bloom filter. The default value is `Hash.MURMUR_HASH`.
- distance.conversion.enabled: When set to `true`, the prograpm will compute the Adamic-Adar _distance_. The deafult value if `false`.

## Running
- For the exact Adamic-Adar computation: `hadoop jar $OKAPI_JAR org.apache.giraph.GiraphRunner ml.grafos.okapi.graphs.AdamicAdarWeighting\$ComputeLogOfInverseDegree -mc ml.grafos.okapi.graphs.AdamicAdarWeighting\$MasterCompute -eif ml.grafos.okapi.io.formats.LongDoubleZerosTextEdgeInputFormat -eip $EDGES_INPUT -eof org.apache.giraph.io.formats.SrcIdDstIdEdgeValueTextOutputFormat -op $EDGE_OUTPUT -ca giraph.oneToAllMsgSending=true -ca giraph.outEdgesClass=org.apache.giraph.edge.HashMapEdges`
- For the approximate Adamic-Adar computation: `hadoop jar $OKAPI_JAR org.apache.giraph.GiraphRunner ml.grafos.okapi.graphs.AdamicAdarWeighting\$ComputeLogOfInverseDegree -mc ml.grafos.okapi.graphs.AdamicAdarWeighting\$MasterCompute -eif ml.grafos.okapi.io.formats.LongDoubleZerosTextEdgeInputFormat -eip $EDGES_INPUT -eof org.apache.giraph.io.formats.SrcIdDstIdEdgeValueTextOutputFormat -op $EDGE_OUTPUT -ca giraph.oneToAllMsgSending=true -ca giraph.outEdgesClass=org.apache.giraph.edge.HashMapEdges -ca adamicadar.approximation.enabled=true`

## References
- Lada A. Adamic and Eytan Adar. Friends and neighbors on the web. Social Networks, 25(3):211–230, July 2003.

