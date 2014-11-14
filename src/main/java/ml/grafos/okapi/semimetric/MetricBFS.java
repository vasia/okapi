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
package ml.grafos.okapi.semimetric;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

/**
 * Implementation of the last step of the backbone algorithm.
 * The input is an unlabeled edge, (u, v) with a weight.
 * u acts as the source vertex of a modified SSSP, which only
 * explores paths that have weight lower than the weight of (u, v).
 * 
 */
public class MetricBFS extends BasicComputation<LongWritable, 
	DoubleWritable, DoubleWritable, DoubleWritable> {
  /** The source id */
  public static final String SOURCE_ID = "bfs.source.id";
  /** The source id */
  public static final String TARGET_ID = "bfs.target.id";
  /** The weight of the unlabeled edge **/
  public static final String EDGE_WEIGHT = "bfs.edge.weight";
  
  private long targetID;
  private float edgeWeight; 
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(MetricBFS.class);

  /**
   * Is this vertex the source id?
   *
   * @return True if the source id
   */
  private boolean isSource(Vertex<LongWritable, DoubleWritable, DoubleWritable> vertex) {
    return vertex.getId().get() ==
        getContext().getConfiguration().getLong(SOURCE_ID, -1);
  }
  
  public void preSuperstep() {
	  edgeWeight = getContext().getConfiguration().getFloat(EDGE_WEIGHT, -1);
	  targetID = getContext().getConfiguration().getLong(TARGET_ID, -1);
  };
  
  @Override
  public void compute(Vertex<LongWritable, DoubleWritable, DoubleWritable> vertex,
      Iterable<DoubleWritable> messages) {
    if (getSuperstep() == 0) {
    	
    	// remove the unlabeled edge from the graph
    	if(isSource(vertex)) {
    		vertex.removeEdges(new LongWritable(targetID));
    	}
    	vertex.setValue(new DoubleWritable(Double.MAX_VALUE));
    }
    
    else { 
	    // In directed graphs, vertices that have no outgoing edges will be created
	    // in the 2nd superstep as a result of messages sent to them.
	    if (getSuperstep() == 2 && vertex.getNumEdges() == 0) {
	      vertex.setValue(new DoubleWritable(Double.MAX_VALUE));
	    }
	
	    double minDist = isSource(vertex) ? 0d : Double.MAX_VALUE;
	    for (DoubleWritable message : messages) {
	      minDist = Math.min(minDist, message.get());
	    }
	    if (LOG.isDebugEnabled()) {
	      LOG.debug("Vertex " + vertex.getId() + " got minDist = " + minDist +
	          " vertex value = " + vertex.getValue());
	    }
	    if (minDist < vertex.getValue().get()) {
	    	
	      vertex.setValue(new DoubleWritable(minDist));
	      
	      for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
	        
	    	  double distance = minDist + edge.getValue().get();
	        
	        if (distance < edgeWeight){
	        	if (LOG.isDebugEnabled()) {
	                LOG.debug("Vertex " + vertex.getId() + " sent to " +
	                    edge.getTargetVertexId() + " = " + distance);
	              }
	        	sendMessage(edge.getTargetVertexId(), new DoubleWritable(distance));
	        }
	        else {
	        	// we don't need to check other edges
	        	// they all have larger weights
	        	break;
	        }
	      }
	    }
	    vertex.voteToHalt();
	  }
  }
}
