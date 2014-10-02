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

import ml.grafos.okapi.incremental.util.FloatBooleanPairWritable;
import ml.grafos.okapi.incremental.util.LongWithDoubleWritable;

import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Program to run after SSSP execution, in order to annotate the edges
 * that belong to the SP-subgraph.
 */

public class AnnotateShortestPathsEdges {
	
	public static class SendIdToNeighbors extends BasicComputation<LongWritable, 
		DoubleWritable, FloatBooleanPairWritable, LongWritable> {

		@Override
		public void compute(
				Vertex<LongWritable, DoubleWritable, FloatBooleanPairWritable> vertex,
				Iterable<LongWritable> messages) throws IOException {
			 	sendMessageToAllEdges(vertex,  vertex.getId());
		}
	}
	
	/**
	 * 
	 * This computation is executed after the convergence of sssp
	 * Each vertex receives the ids of all its neighbors
	 * and sends them its sssp distance.
	 *
	 */
	public static class SendDistanceToInNeighbors extends AbstractComputation<LongWritable, 
		DoubleWritable, FloatBooleanPairWritable, LongWritable, LongWithDoubleWritable> {

		@Override
		public void compute(
				Vertex<LongWritable, DoubleWritable, FloatBooleanPairWritable> vertex,
				Iterable<LongWritable> messages) throws IOException {
			for (LongWritable inNeighborID: messages) {
				sendMessage(inNeighborID, new LongWithDoubleWritable(vertex.getId(), vertex.getValue()));
			}
		}
	}
	
	public static class AnnotateSPEdges extends BasicComputation<LongWritable, 
		DoubleWritable, FloatBooleanPairWritable, LongWithDoubleWritable> {

		@Override
		public void compute(
				Vertex<LongWritable, DoubleWritable, FloatBooleanPairWritable> vertex,
				Iterable<LongWithDoubleWritable> messages) throws IOException {
			for (LongWithDoubleWritable msg: messages) {
				// check if the edge (vertexID -> msgSenderID) is an SP-G edge
				// i.e. if sender_distance = own_distance + edge_weight
				LongWritable senderId = msg.getVertexId();
				FloatBooleanPairWritable edgeValue = vertex.getEdgeValue(senderId); 
				float edgeWeight = edgeValue.getWeight();
				if (msg.getVertexValue().get() == (vertex.getValue().get() + edgeWeight)) {
					vertex.setEdgeValue(senderId, edgeValue.setSPEdge(true));				
				}
			}
			vertex.voteToHalt();
		}
	}
	
	  /**
	   * Coordinates the execution of the algorithm.
	   */
	  public static class MasterCompute extends DefaultMasterCompute {
		  
		  @Override
		    public final void compute() {
		      long superstep = getSuperstep();
		      if (superstep == 0) {
		        setComputation(SendIdToNeighbors.class);
		      } else if (superstep == 1){
		    	  setComputation(SendDistanceToInNeighbors.class);
		    	  setIncomingMessage(LongWritable.class);
		    	  setOutgoingMessage(LongWithDoubleWritable.class);
		      }
		      else {
		    	  setComputation(AnnotateSPEdges.class);
		      }
		  }
	  }
	  
}
