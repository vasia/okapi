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
import java.util.HashSet;
import java.util.Set;

import ml.grafos.okapi.semimetric.SecondStepSemimetric.DoubleBooleanPair;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Implementation of the last step of the backbone algorithm.
 * The input is an unlabeled edge, (u, v) with a weight.
 * u acts as the source vertex of a modified SSSP, which only
 * explores paths that have weight lower than the weight of (u, v).
 * 
 * IMPORTANT NOTE: This implementation assumes that the type of the OutEdges
 * is ml.grafos.okapi.semimetric.common.TreeSetOutEdges.
 */
public class MetricBFS {
	
	/** The unlabeled edges aggregator*/
	public static final String UNLABELED_EDGES_AGGREGATOR = "unlabeled.edges.aggregator";
	/** The aggregator that contains the edge currently under consideration*/
	public static final String CURRENT_EDGE_AGGREGATOR = "current.edge.aggregator";
	/** The aggregator that contains the edge that will be potentially removed*/
	public static final String EDGE_TO_REMOVE_AGGREGATOR = "edge.to.remove.aggregator";
	/** The aggregator used to mark the initialization superstep of the custom BFS*/
	public static final String BFS_START_AGGREGATOR = "bfs.startsuperstep.aggregator";
	/** The aggregator used to check whether an instance of the custom BFS has converged*/
	public static final String CHECK_CONVERGENCE_AGGREGATOR = "check.convergence.aggregator";
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
		DoubleWritable, DoubleBooleanPair, DoubleWritable> {

		@Override
		public void compute(
				Vertex<LongWritable, DoubleWritable, DoubleBooleanPair> vertex,
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
	 * it checks whether there exists a shortest indirect path
	 * in the graph. 
	 * If yes, the edge is semi-metric and, thus, deleted from the graph.
	 * Otherwise, the edge is marked as metric.
	 */
	public static class CustomBFS extends BasicComputation<LongWritable, 
		DoubleWritable, DoubleBooleanPair, DoubleWritable> {

	  private long sourceId;
	  private long targetID;
	  private double edgeWeight; 
	  
	  public void preSuperstep() {
		  UnlabeledEdge edgeToCheck = getAggregatedValue(CURRENT_EDGE_AGGREGATOR);
		  sourceId = edgeToCheck.getSource();
		  targetID = edgeToCheck.getTarget();
		  edgeWeight = edgeToCheck.getWeight();
	  };
	  
	  @Override
	  public void compute(Vertex<LongWritable, DoubleWritable, DoubleBooleanPair> vertex,
	      Iterable<DoubleWritable> messages) {

		  long bfsStartingSuperstep = ((LongWritable)getAggregatedValue(BFS_START_AGGREGATOR)).get();

		  if (getSuperstep() == bfsStartingSuperstep) {
	    	// remove the unlabeled edge from the graph
	    	if(vertex.getId().get() == sourceId) {
	    		vertex.removeEdges(new LongWritable(targetID));
	    		aggregate(CHECK_CONVERGENCE_AGGREGATOR, new BooleanWritable(false));
	    	}
	    	vertex.setValue(new DoubleWritable(Double.MAX_VALUE));
	    }
	    else {
		    double minDist = (vertex.getId().get() == sourceId) ? 0d : Double.MAX_VALUE;
		    for (DoubleWritable message : messages) {
		      minDist = Math.min(minDist, message.get());
		    }
		    
		    if (minDist < vertex.getValue().get()) {
		      vertex.setValue(new DoubleWritable(minDist));
		      // the vertex changed value: set the convergence aggregator to false
		      aggregate(CHECK_CONVERGENCE_AGGREGATOR, new BooleanWritable(false));
		      
		      for (Edge<LongWritable, DoubleBooleanPair> edge : vertex.getEdges()) {
		    	  double distance = minDist + edge.getValue().getWeight(); 
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
		  }
	  }
	}
	
	/**
	 * Computation class that is executed after the convergence 
	 * of one round of the custom BFS. 
	 * During this superstep, the responsible vertex checks whether
	 * the edge under consideration was found to be metric or semi-metric.
	 * this edge has been removed at the beginning of the custom NFS.
	 * If the edge is found to be semi-metric, the opposite-direction edge 
	 * is removed from the graph.
	 * Otherwise, it is added back.
	 */
	public static class RemoveSemimetricEdge extends BasicComputation<LongWritable, 
		DoubleWritable, DoubleBooleanPair, DoubleWritable> {

		@Override
		public void compute(
				Vertex<LongWritable, DoubleWritable, DoubleBooleanPair> vertex,
				Iterable<DoubleWritable> messages) throws IOException {

			UnlabeledEdge currentEdge = getAggregatedValue(EDGE_TO_REMOVE_AGGREGATOR);
			// compare the weight of the edge with the distance of the target vertex
			// if the weight is lower than the target value, the edge is labeled as metric
			// otherwise it is removed together with the opposite-direction edge
			long sourceID = currentEdge.getSource();
			long targetID = currentEdge.getTarget();
			double edgeWeight = currentEdge.getWeight();
			LongWritable sourceVertex = new LongWritable(sourceID);
			LongWritable targetVertex = new LongWritable(targetID);

			if (vertex.getId().get() == targetID) {
				if (vertex.getValue().get() < edgeWeight) {
					// the edge is semi-metric: remove the opposite-direction edge
					removeEdgesRequest(targetVertex, sourceVertex);
				}
				else {
					// the edge was found to be metric: add it back
					addEdgeRequest(sourceVertex, 
							EdgeFactory.create(targetVertex, new DoubleBooleanPair(edgeWeight, true)));
				}
			}
		  }
	  }
	
	/** 
	 * Last essential superstep for the edge add/remove requests to be executed
	 */
	public static class Finalize extends BasicComputation<LongWritable, 
		DoubleWritable, DoubleBooleanPair, DoubleWritable> {

		@Override
		public void compute(
				Vertex<LongWritable, DoubleWritable, DoubleBooleanPair> vertex,
				Iterable<DoubleWritable> messages) throws IOException {
				vertex.voteToHalt();
		}
		
	}

	 /**
	   * 
	   * MastercCompute coordinates the execution.
	   * A custom BFS is initiated for each unlabeled edge,
	   * until all the edges of the graph have been either 
	   * labeled as metric or have been removed.
	   *
	   */
	  public static class MasterCompute extends DefaultMasterCompute {
		  
		  private long lastsuperstep = -1;
	    
		  // use a HashSet for easy removal of opposite-direction edges
		private Set<UnlabeledEdge> unlabeledEdges = new HashSet<UnlabeledEdge>();  
		  
		@Override
	    public final void initialize() throws InstantiationException, IllegalAccessException {			
			// register the current edge aggregator
			registerPersistentAggregator(CURRENT_EDGE_AGGREGATOR, SingleUnlabeledEdgeAggregator.class);
			// register the edge to remove aggregator
			registerPersistentAggregator(EDGE_TO_REMOVE_AGGREGATOR, SingleUnlabeledEdgeAggregator.class);
			// register the convergence aggregator
			registerPersistentAggregator(CHECK_CONVERGENCE_AGGREGATOR, BooleanAndAggregator.class);
			// register the custom bfs start aggregator
			registerPersistentAggregator(BFS_START_AGGREGATOR, LongSumAggregator.class);
			// register unlabeled edges aggregator
	    	registerAggregator(UNLABELED_EDGES_AGGREGATOR, UnlabeledEdgesAggregator.class);
		}

	    @Override
	    public void compute() {

	      long superstep = getSuperstep();

	      // check for program termination
	      if ((superstep == lastsuperstep) && (superstep > 1)) {
	    	  haltComputation();
	      }
	      
	      if (superstep == 0) {
	    	  setComputation(PutUnlabeledEdgesInAggregator.class);
	      }
	      else if (superstep == 1) {
				// copy the set of unlabeled edges locally
				unlabeledEdges = getAggregatedValue(UNLABELED_EDGES_AGGREGATOR);
				
				if (!(unlabeledEdges.isEmpty())) {
					// set the value of the current edge aggregator
					setAggregatedValue(CURRENT_EDGE_AGGREGATOR, unlabeledEdges.iterator().next());
					
					// BFS has converged: set the BFS superstep aggregator
		    		setAggregatedValue(BFS_START_AGGREGATOR, new LongWritable(1));
		    		  
					//start the customBFS
					setComputation(CustomBFS.class);
				}
				else {
					// no more edges to consider --> halt computation
					// set the last superstep (if not already set)
					if (lastsuperstep == -1) {
						lastsuperstep = getSuperstep()+2;
					}
					setComputation(Finalize.class);
				}
	      }
	      else if(superstep > 1) {
	    	  if (unlabeledEdges.isEmpty()) {
	    		// set the last superstep (if not already set)
	    		  if (lastsuperstep == -1) {
						lastsuperstep = getSuperstep()+2;
	    		  }
		    		setComputation(Finalize.class);
	    	  }
	    	  else {
		    	  // check convergenceAggregator
		    	  if (((BooleanWritable)getAggregatedValue(CHECK_CONVERGENCE_AGGREGATOR)).get()) {

		    		  // BFS has converged: set the BFS superstep aggregator
		    		  setAggregatedValue(BFS_START_AGGREGATOR, new LongWritable(superstep+2));
	
		    		  // remove the previous edge and its opposite-direction edge from the list
		    		  UnlabeledEdge edgeToRemove  = getAggregatedValue(CURRENT_EDGE_AGGREGATOR);
		    		  // set the edge to remove aggregator
		    		  setAggregatedValue(EDGE_TO_REMOVE_AGGREGATOR, edgeToRemove);
		    		  unlabeledEdges.remove(edgeToRemove);
		    		  unlabeledEdges.remove(edgeToRemove.oppositeDirectionEdge());
		    		  
		    		  // set the current edge aggregator
		    		  if (!(unlabeledEdges.isEmpty())) {
							// proceed to the next edge in the set
							setAggregatedValue(CURRENT_EDGE_AGGREGATOR, unlabeledEdges.iterator().next());
		    		  }
		    		  else {
		    			  setAggregatedValue(CURRENT_EDGE_AGGREGATOR, new UnlabeledEdge(-1, -1, -1d));
		    		  }
	
		    		  // set the convergence aggregator
		    		  setAggregatedValue(CHECK_CONVERGENCE_AGGREGATOR, new BooleanWritable(false));

		    		  // the custom BFS has converged --> remove the edge if semi-metric
		    		  setComputation(RemoveSemimetricEdge.class);
		    	  }
		    	  else {
		    		  long edgeSrcID = ((UnlabeledEdge)(getAggregatedValue(CURRENT_EDGE_AGGREGATOR))).getSource();
		    		  if (edgeSrcID == -1) {
		    			  setComputation(Finalize.class);
		    			  // set the last superstep
		    			  if (lastsuperstep == -1) {
								lastsuperstep = getSuperstep()+2;
		    			  }
		    		  }
		    		  else {
			    		  // set the convergence aggregator
			    		  setAggregatedValue(CHECK_CONVERGENCE_AGGREGATOR, new BooleanWritable(true));
			    		  // run the customBFS
			    		  setComputation(CustomBFS.class);
		    		  }
		    	  }
		      }
	      }
	    }
	  }
}