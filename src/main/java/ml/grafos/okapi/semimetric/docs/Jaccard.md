Jaccard
========

## Overview
The Jaccard similarity represents how similar two nodes of a graph are, based on their common neighbors. In particular, it is computed as the size of the intersection of the nodes’ neighbor sets over the size of the union of the nodes’ neighbor sets. The similarity value between two nodes is a number in the range [0, 1]. The closer the value is to 1, the more similar the nodes. The same metric can be easily converted to a distance with values in the range [0, INF]. This implementation only computes similarity or distance for existing edges of the input graph and not for every pair of nodes.

## Details
The computation consists of 2 supersteps. During the first superstep, each vertex sends a list of its friends to all its neighbors in the graph. In the second superstep, each vertex computes the Jaccard similarity value for each of its edges. For every message received, it compares the friend list with its own neighborhood and computes common and total number of friends. It then sets the edge value to the Jaccard similarity index. If the configuration option for distance conversion is enabled, then an additional superstep is executed, where the Jaccard index is converted to a distance index, using the function `f(x) = (1/x) - 1`.
If the input graph is large, the computation of the Jaccard index for all the edges might be very expensive. For this reason, an approximate implementation is also provided. In this implementation, each vertex uses a BloomFilter to store its neighborhood and sends this filter as a message to its neighbors, instead of the exact friend list. The hash functions to use and the size of the BloomFilter are configurable. 

## How to Run

### Input Format
The input format is an edge list, containing one edge per line, in the format `srcID\ttargetID` where the IDs are of type Long.

### Output Format
The output of this program is the edges of the graph with their Jaccard index, one edge per line, in the form:
		srcID\ttargetID\tsimilarity
where the IDs are of type Long and similarity is Double.

## Configuration Parameters
- jaccard.approximation.enabled: When set to `true`, the bloom filter implementation will be used and the program will compute the approximate Jaccard index. The deafult value is `false`.
- jaccard.bloom.filter.bits: Size of the bloom filter, in bits. The default value if 16.
- jaccard.bloom.filter.functions: The number of hash functions to use for the bloom filter. The default value is 1.
- jaccard.bloom.filter.hash.type: The type of the hash functions to use for the bloom filter. The default value is `Hash.MURMURHASH`.
- distance.conversion.enabled: When set to `true`, the prograpm will compute the Jaccard _distance_. The deafult value if `false`.

## Running
`hadoop jar $OKAPI_JAR org.apache.giraph.GiraphRunner ml.grafos.okapi.graphs.Similarity\$SendFriendsBloomFilter -mc ml.grafos.okapi.graphs.Similarity\$MasterCompute -eif ml.grafos.okapi.io.formats.LongDoubleZerosTextEdgeInputFormat -eip $EDGES_INPUT -eof org.apache.giraph.io.formats.SrcIdDstIdEdgeValueTextOutputFormat -op $EDGE_OUTPUT -ca giraph.oneToAllMsgSending=true -ca giraph.outEdgesClass=org.apache.giraph.edge.HashMapEdges`

## References
- http://en.wikipedia.org/wiki/Jaccard_index
