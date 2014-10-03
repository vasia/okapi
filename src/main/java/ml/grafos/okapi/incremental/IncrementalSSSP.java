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
package ml.grafos.okapi.incremental;

import java.io.IOException;

import ml.grafos.okapi.incremental.util.DoubleLongPairWritable;
import ml.grafos.okapi.incremental.util.FloatBooleanPairWritable;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * 
 * Incremental SSSP computation.
 * 
 * The program takes as input the result graph of a SSSP computation
 * (i.e. vertices have their final values, after convergence)
 * and an event type as a configuration parameter.
 * The event type corresponds to the graph structure modification
 * and can either be an EDGE_ADDITION or an EDGE_REMOVAL.
 * The src vertex id and the trg vertex id of the edge to be added/removed
 * also needs to be specified as a configuration parameter.
 * 
 * 1. EDGE_REMOVAL: 
 * - If the removed edge does not belong to the SP-graph, no computation is necessary.
 * The edge is simply removed from the graph.
 * - If the removed edge is an SP-edge, then all nodes, whose shortest path contains the removed edge,
 * potentially require re-computation.
 * When the edge <u, v> is removed, v checks if it has another in-coming SP-edge.
 * If yes, no further computation is required.
 * If v has no other in-coming SP-edge, it invalidates its current value, by setting it to INF.
 * Then, it informs all its SP-out-neighbors by sending them an INVALIDATE message.
 * When a vertex u receives an INVALIDATE message from v, it checks whether it has another in-coming SP-edge.
 * If not, it invalidates its current value and propagates the INVALIDATE message.
 * The propagation stops when a vertex with an alternative shortest path is reached
 * or when we reach a vertex with no SP-out-neighbors.
 * - When all the affected vertices have been located, the normal SSSP computation is executed, until convergence.
 * - After convergence, we annotate the SP-edges of the affected vertices.
 * 
 * 2. EDGE_ADDITION:
 * - When an edge <u, v> with weight w is added to the graph,
 *  -- if (v.value < w + u.value), we simply add the edge to the graph and halt.
 *  -- if (v.value = w + u.value), then we add the edge to the graph and annotate it as an SP-edge.
 *  -- if (v.value > w + u.value), then v updates its value to w + u.value. then, starting from v, 
 *     we execute normal SSSP, until convergence. 
 *     After convergence we annotate the SP-edges of the affected vertices.
 * 
 */

public class IncrementalSSSP {
	
	/** The event type that triggers the incremental computation.
	 * It can take two values:  1 for EDGE_ADDITION or 2 for EDGE_REMOVAL */
	 private static final String EVENT_TYPE = "event.type";
	 
	 /** The source vertex id of the edge to be added/removed */
	 private static final String EVENT_EDGE_SRC = "event.edge.src";
	 
	 /** The target vertex id of the edge to be added/removed */
	 private static final String EVENT_EDGE_TRG = "event.edge.trg";
	 
	 /** The INVALIDATE message is a simple integer */
	 private static final IntWritable INVALIDATE = new IntWritable(-1);
	 
	 private static final int EDGE_ADDITION = 1;
	 
	 private static final int EDGE_REMOVAL = 2;

	 
	 /**
	  * 
	  * This is the first step when the event type is an edge removal.
	  * The source vertex checks whether the edge belongs to the SP-G.
	  * If not, it simply removes the edge and halts.
	  * Otherwise, it removes the edge and then activates the target vertex.
	  * 
	  */
	 public static class CheckIfEdgeInSPG extends BasicComputation<LongWritable, DoubleLongPairWritable, 
	 	FloatBooleanPairWritable, IntWritable> {

		private long edgeSrc;
		private long edgeTrg;
		 
		@Override
		public void preSuperstep() {
			edgeSrc = getContext().getConfiguration().getLong(EVENT_EDGE_SRC, -1L);
			edgeTrg = getContext().getConfiguration().getLong(EVENT_EDGE_TRG, -1L);
		};
		
		@Override
		public void compute(
				Vertex<LongWritable, DoubleLongPairWritable, FloatBooleanPairWritable> vertex,
				Iterable<IntWritable> messages) throws IOException {
			LongWritable trgVertexId = new LongWritable(edgeTrg);
			
			if (vertex.getId().get() == edgeSrc) {
				if (vertex.getEdgeValue(trgVertexId).belongsToSPG()) {
					// activate the edge target
					sendMessage(trgVertexId, INVALIDATE);
				}
				//remove the edge
				vertex.removeEdges(trgVertexId);
			}
			vertex.voteToHalt();
		}
	 
	 }
	 
	 /**
	  * 
	  * This is the second step in the incremental SSSP computation.
	  * The edge target vertex checks whether computation is required 
	  * and then propagates an INVALIDATE message (the event is an edge removal)
	  *
	  */
	 public static class PropagateInvalidate extends BasicComputation<LongWritable, DoubleLongPairWritable, 
	 	FloatBooleanPairWritable, IntWritable> {
		
		@Override
		public void compute(
				Vertex<LongWritable, DoubleLongPairWritable, FloatBooleanPairWritable> vertex,
				Iterable<IntWritable> messages) throws IOException {

			/**
			 *  During superstep 1, only the edgeTrg vertex receives a msg
			 *  All the other vertices have voted to halt.
			 *  In supersteps > 1, this will be executed by the affected vertices
			 *  which received an INVALIDATE message.
			 */ 
			// TODO: add an aggregator to store how many vertices reset their distances to infinity
			// and decide whether to run the normal SSSP based on this value.

			vertex.setValue(vertex.getValue().decrementInSPdegree());
			// check if the vertex has another incoming-SP-Edge
			if (vertex.getValue().getInSPdegree() > 0) {
				// there exists another shortest path from the source to this vertex
				;
			}
			else {
				// set own value to infinity
				vertex.setValue(vertex.getValue().setDistance(Double.MAX_VALUE));
				// invalidate all out-SP-edges
				for (Edge<LongWritable, FloatBooleanPairWritable> edge : vertex.getEdges()) {
					if (edge.getValue().belongsToSPG()) {
						sendMessage(edge.getTargetVertexId(), INVALIDATE);
					}
				}
			}
			vertex.voteToHalt();
		}
	 }
	 
	 /**
	   * Coordinates the execution of the algorithm.
	   */
	  public static class MasterCompute extends DefaultMasterCompute {
		  
		  private int eventType;
		  
		  @Override
		  public void initialize() throws InstantiationException, IllegalAccessException {
			  eventType = getContext().getConfiguration().getInt(EVENT_TYPE, -1);
		  }
		  
		  @Override
		  public final void compute() {
			  
		    long superstep = getSuperstep();
		    if (eventType == EDGE_REMOVAL) {
			    if (superstep == 0) {
			    	setComputation(CheckIfEdgeInSPG.class);
				} else if (superstep == 1) {
					setComputation(PropagateInvalidate.class);  
				} else {
					
				}
		    } else if (eventType == EDGE_ADDITION) {
		    	
		    } else {
		    	System.err.print("Unkown event type");
		    	System.exit(-1);
		    }
		  }
	  }
	  
}
