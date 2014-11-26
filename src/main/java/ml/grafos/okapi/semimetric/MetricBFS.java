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

import java.io.IOException;

import ml.grafos.okapi.semimetric.SecondStepSemimetric.DoubleBooleanPair;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * Implementation of the last step of the backbone algorithm.
 * The input is an unlabeled edge, (u, v) with a weight.
 * u acts as the source vertex of a modified SSSP, which only
 * explores paths that have weight lower than the weight of (u, v).
 * 
 */
public class MetricBFS {
	
	/** The unlabeled edges aggregator*/
	public static final String UNLABELED_EDGES_AGGREGATOR = "unlabeled.edges.aggregator";
	/** The source id */
	public static final String SOURCE_ID = "bfs.source.id";
	/** The source id */
	public static final String TARGET_ID = "bfs.target.id";
	/** The weight of the unlabeled edge **/
	public static final String EDGE_WEIGHT = "bfs.edge.weight";

	
	/**
	 * Initialization Computation class.
	 * Every vertex appends its unlabeled edges
	 * to the unlabeled-edge aggregator.
	 */
	public static class PutUnlabeledEdgesInAggregator extends BasicComputation<LongWritable, 
		NullWritable, DoubleBooleanPair, DoubleWritable> {

		@Override
		public void compute(
				Vertex<LongWritable, NullWritable, DoubleBooleanPair> vertex,
				Iterable<DoubleWritable> messages) throws IOException {
			for (Edge<LongWritable, DoubleBooleanPair> e : vertex.getEdges()) {
				DoubleBooleanPair edgeValue = e.getValue();
				if (!(edgeValue.isMetric())) {
					UnlabeledEdge edgeToAdd = new UnlabeledEdge(vertex.getId().get(), e.getTargetVertexId().get(),
							e.getValue().getWeight());
					UnlabeledEdgeHashSetWritable aggrSet = new UnlabeledEdgeHashSetWritable();
					aggrSet.add(edgeToAdd);
					aggregate(UNLABELED_EDGES_AGGREGATOR, aggrSet);
				}
			}
		}
	}
	
	/**
	 * Computation class for the custom BFS.
	 * Given a weighted, unlabeled edge as input, 
	 * it checks whether there exists a shortes indirect path
	 * in the graph. 
	 * If yes, the edge is semi-metric and, thus, deleted from the graph.
	 * Otherwise, the edge is marked as metric.
	 */
	public static class CustomBFS extends BasicComputation<LongWritable, 
		DoubleWritable, DoubleWritable, DoubleWritable> {

	  private long sourceId;
	  private long targetID;
	  private double edgeWeight; 
	  
	  public void preSuperstep() {
		  UnlabeledEdgeHashSetWritable aggrValue = getAggregatedValue(UNLABELED_EDGES_AGGREGATOR);
		  UnlabeledEdge edgeToCheck = aggrValue.iterator().next();
		  sourceId = edgeToCheck.getSource();
		  targetID = edgeToCheck.getTarget();
		  edgeWeight = edgeToCheck.getWeight();
	  };
	  
	  @Override
	  public void compute(Vertex<LongWritable, DoubleWritable, DoubleWritable> vertex,
	      Iterable<DoubleWritable> messages) {
	    if (getSuperstep() == 0) {
	    	
	    	// remove the unlabeled edge from the graph
	    	if((vertex.getId().compareTo(sourceId) == 0)) {
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
		
		    double minDist = (vertex.getId().compareTo(sourceId) == 0) ? 0d : Double.MAX_VALUE;
		    for (DoubleWritable message : messages) {
		      minDist = Math.min(minDist, message.get());
		    }
		    
		    if (minDist < vertex.getValue().get()) {
		    	
		      vertex.setValue(new DoubleWritable(minDist));
		      
		      for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
		        
		    	  double distance = minDist + edge.getValue().get();
		        
		        if (distance < edgeWeight){
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
	
	 /**
	   * 
	   * MastercCompute coordinates the execution.
	   * A custom BFS is initiated for each unlabeled edge,
	   * until all the edges of the graph have been labeled as metric
	   * or have been removed.
	   *
	   */
	  public static class MasterCompute extends DefaultMasterCompute {
	    
		@Override
	    public final void initialize() throws InstantiationException,
	        IllegalAccessException {
	    	// register unlabeled edges aggregator
	    	registerPersistentAggregator(UNLABELED_EDGES_AGGREGATOR, UnlabeledEdgesAggregator.class);
		}
		  
	    @Override
	    public void compute() {

	      long superstep = getSuperstep();
	      if (superstep==0) {
	    	  setComputation(PutUnlabeledEdgesInAggregator.class);
	      } else {
	    	  // if the aggregator is empty, there are no more unlabeled edges -> halt
	    	  UnlabeledEdgeHashSetWritable aggrValue = getAggregatedValue(UNLABELED_EDGES_AGGREGATOR);
	    	  if (aggrValue.isEmpty()) {
	    		  haltComputation();
	    	  }
	    	  else  {
	    	  setComputation(CustomBFS.class);
	    	  }
	      }
	    }
	  }
}
