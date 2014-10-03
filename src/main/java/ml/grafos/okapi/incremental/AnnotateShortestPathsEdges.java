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
		DoubleLongPairWritable, FloatBooleanPairWritable, LongWritable> {

		@Override
		public void compute(
				Vertex<LongWritable, DoubleLongPairWritable, FloatBooleanPairWritable> vertex,
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
		DoubleLongPairWritable, FloatBooleanPairWritable, LongWritable, LongWithDoubleWritable> {

		@Override
		public void compute(
				Vertex<LongWritable, DoubleLongPairWritable, FloatBooleanPairWritable> vertex,
				Iterable<LongWritable> messages) throws IOException {
			for (LongWritable inNeighborID: messages) {
				sendMessage(inNeighborID, new LongWithDoubleWritable(vertex.getId(), 
						new DoubleWritable(vertex.getValue().getDistance())));
			}
		}
	}
	
	public static class AnnotateSPEdges extends AbstractComputation<LongWritable, 
		DoubleLongPairWritable, FloatBooleanPairWritable, LongWithDoubleWritable,
		LongWritable> {

		@Override
		public void compute(
				Vertex<LongWritable, DoubleLongPairWritable, FloatBooleanPairWritable> vertex,
				Iterable<LongWithDoubleWritable> messages) throws IOException {
			for (LongWithDoubleWritable msg: messages) {
				// check if the edge (vertexID -> msgSenderID) is an SP-G edge
				// i.e. if sender_distance = own_distance + edge_weight
				LongWritable senderId = msg.getVertexId();
				FloatBooleanPairWritable edgeValue = vertex.getEdgeValue(senderId); 
				float edgeWeight = edgeValue.getWeight();
				if (msg.getVertexValue().get() == (vertex.getValue().getDistance() + edgeWeight)) {
					vertex.setEdgeValue(senderId, edgeValue.setSPEdge(true));
					
					// send a message to the sender to increase its in-SP-degree
					sendMessage(senderId, new LongWritable(1));
				}
			}
		}
	}
	
	/**
	 * 
	 * In this last step, each vertex aggregates the received messages
	 * and sets its in-SP-degree to the sum of them.
	 *
	 */
	public static class ComputeInSPEdgeDegree extends BasicComputation<LongWritable, 
	DoubleLongPairWritable, FloatBooleanPairWritable, LongWritable> {

		@Override
		public void compute(
				Vertex<LongWritable, DoubleLongPairWritable, FloatBooleanPairWritable> vertex,
				Iterable<LongWritable> messages) throws IOException {
			long sum = 0;
			for (LongWritable msg :  messages) {
				sum += msg.get();
			}
			vertex.setValue(vertex.getValue().setInSPdegree(sum));
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
		      } else if (superstep == 1) {
		    	  setComputation(SendDistanceToInNeighbors.class);
		    	  setIncomingMessage(LongWritable.class);
		    	  setOutgoingMessage(LongWithDoubleWritable.class);
		      }
		      else if (superstep == 2) {
		    	  setComputation(AnnotateSPEdges.class);
		    	  setIncomingMessage(LongWithDoubleWritable.class);
		    	  setOutgoingMessage(LongWritable.class);
		      }
		      else {
		    	  setComputation(ComputeInSPEdgeDegree.class);
		      }
		  }
	  }
	  
}
