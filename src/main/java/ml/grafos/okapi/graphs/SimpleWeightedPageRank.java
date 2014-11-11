/**
 * Copyright 2014 Grafos.ml
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ml.grafos.okapi.graphs;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Weighted PageRank implementation.
 *
 * This version initializes the value of every vertex to 1/N, where N is the
 * total number of vertices.
 * The partial rank of each vertex is computed by multiplying the rank
 * with the edge weight and dividing the result by the sum of weights
 * on all out-going edges of this vertex.
 *
 * The maximum number of supersteps is configurable.
 */
public class SimpleWeightedPageRank extends BasicComputation<LongWritable,
  DoubleWritable, DoubleWritable, DoubleWritable> {
  /** Default number of supersteps */
  public static final int MAX_SUPERSTEPS_DEFAULT = 30;
  /** Property name for number of supersteps */
  public static final String MAX_SUPERSTEPS = "pagerank.max.supersteps";

  @Override
  public void compute(
      Vertex<LongWritable, DoubleWritable, DoubleWritable> vertex,
      Iterable<DoubleWritable> messages) {
    if (getSuperstep() == 0) {
      vertex.setValue(new DoubleWritable(1.0 / getTotalNumVertices()));
    }
    if (getSuperstep() >= 1) {
      double sum = 0;
      for (DoubleWritable message : messages) {
        sum += message.get();
      }
      DoubleWritable vertexValue =
        new DoubleWritable((0.15 / getTotalNumVertices()) + 0.85 * sum);
      vertex.setValue(vertexValue);
    }

    if (getSuperstep() < getContext().getConfiguration().getInt(
      MAX_SUPERSTEPS, MAX_SUPERSTEPS_DEFAULT)) {

	    for (Edge<LongWritable, DoubleWritable> e : vertex.getEdges()) {
	    	
	    	DoubleWritable partial_rank = new DoubleWritable();
	    	
	    	if (getSumOfWeights(vertex) > 0 ) {
	    		partial_rank.set(((vertex.getValue().get() * e.getValue().get()) / getSumOfWeights(vertex)));
	    		sendMessage(e.getTargetVertexId(), partial_rank);
	    	}	
	    	}
    	} else {
    		vertex.voteToHalt();
    	}
  }
  
  /**
   * Computes the sum of weights of all out-going edges
   * 
   */
  private static double getSumOfWeights(Vertex<LongWritable, DoubleWritable, DoubleWritable> v) {
	  double sum = 0.0;
	  for (Edge<LongWritable, DoubleWritable> e : v.getEdges()) {
		  sum += e.getValue().get();
	  }
	  return sum;
  }
}
