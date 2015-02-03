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
import java.util.ArrayList;
import java.util.List;

import ml.grafos.okapi.semimetric.common.DoubleIntegerPair;
import ml.grafos.okapi.semimetric.common.LRUMapWritable;
import ml.grafos.okapi.semimetric.common.LRUMapWritableBooleanPair;
import ml.grafos.okapi.semimetric.common.LongLongPair;
import ml.grafos.okapi.semimetric.common.SimpleEdgeWithWeight;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.LongWritable;

/**
 * Implementation of the last step of the backbone algorithm.
 * The input is a graph with some labeled and some unlabeled edges.
 * For every unlabeled edge (u, v), u propagates a message to its neighbors
 * to explore paths that have weight lower than the weight of (u, v).
 * 
 * At the end of the computation, a previously unlabeled edge 
 * has either been marked as semi-metric or is still unlabeled, which means it is metric.
 * 
 * This implementation uses megasteps to spread out the communication costs.
 * This technique reduces the communication and computation costs per superstep,
 * thus, reducing the memory requirements per superstep.
 * 
 * This technique resembles a pipeline scheme:
 * Vertices are grouped by ID and do not initiate the BFSs simultaneously.
 * In every superstep, one group of vertices gets "activated".
 * Only the vertices in this group initiate BFSs for their unlabeled edges,
 * while the rest of the vertices only process received messages.
 * 
 */
public class OptimizedParallelMetricBFS {

	/** Size of the visited LRU map */
	public static final String LRU_MAP_SIZE = "lru.map.size";
	  
	/** Default size of visited LRU map */
	public static final int LRU_MAP_SIZE_DEFAULT = 100;
	
	/** The number of megasteps */
	public static final String MEGASTEPS = "number.of.megasteps";
	  
	/** The default number of megasteps */
	public static final int MEGASTEPS_DEFAULT = 3;

	
	/**
	 * Initialize the LRUMap of the vertices
	 */
	public static class Initialize extends BasicComputation<LongWritable, 
		LRUMapWritableBooleanPair, DoubleIntegerPair, SimpleEdgeWithWeight> {

		int lruMapSize;
		int megasteps;
		
		@Override
		public void preSuperstep() {
			lruMapSize = getContext().getConfiguration().getInt(LRU_MAP_SIZE, LRU_MAP_SIZE_DEFAULT); 
			megasteps = getContext().getConfiguration().getInt(MEGASTEPS, MEGASTEPS_DEFAULT);
		};
		
		@Override
		public void compute(Vertex<LongWritable, LRUMapWritableBooleanPair, DoubleIntegerPair> vertex,
				Iterable<SimpleEdgeWithWeight> messages) throws IOException {

			vertex.setValue(new LRUMapWritableBooleanPair(false, new LRUMapWritable(lruMapSize)));
		}
	}
	
	/**
	 * During each superstep, every vertex that belongs to the "active" group
	 * gathers its unlabeled edges. For each unlabeled edge, it creates a message and propagates it
	 * along all edges that have weight lower than the unlabeled edge weight.
	 * 
	 *  All vertices (in active group or not) process received messages:
	 *  Upon receiving a msg, a vertex checks whether it is the target of the msg edge: 
	 * - if yes, the msg edge is semi-metric.
	 * - otherwise, it propagates the msg to edges that can lead to shortest paths.
	 */
	public static class ParalleBFSCompute extends BasicComputation<LongWritable, 
		LRUMapWritableBooleanPair, DoubleIntegerPair, SimpleEdgeWithWeight> {

		private SimpleEdgeWithWeight msg  = new SimpleEdgeWithWeight();
		int megasteps;
		
		@Override
		public void preSuperstep() { 
			megasteps = getContext().getConfiguration().getInt(MEGASTEPS, MEGASTEPS_DEFAULT);
		};

		@Override
		public void compute(Vertex<LongWritable, LRUMapWritableBooleanPair, DoubleIntegerPair> vertex,
				Iterable<SimpleEdgeWithWeight> messages) throws IOException {

			final long vertexId = vertex.getId().get();
	    	final long superstep = getSuperstep();
	    	boolean hasBeenActive = vertex.getValue().canHalt();
	    	
			/** if active: gather unlabeled edges and initiate a BFS for each one **/
	    	if ((vertexId % megasteps) == (superstep - 1)) {
	    		
	    		hasBeenActive = true;
				// retrieve list of unlabeled edges
				List<Long> unlabeledEdgeTargets = new ArrayList<Long>();
	
				for (Edge<LongWritable, DoubleIntegerPair> e : vertex.getEdges()) {
					if (!(e.getValue().isMetric())) {
						unlabeledEdgeTargets.add(e.getTargetVertexId().get());
					}
				}
	
				// for each unlabeled edge, send a message to all other edges with weight
				// less than the weight of this edge (possible shorter paths)
				for (long target : unlabeledEdgeTargets) {
	
					for (Edge<LongWritable, DoubleIntegerPair> e : vertex.getEdges()) {
						double unlabeledEdgeWeight = vertex.getEdgeValue(new LongWritable(target)).getWeight(); 
						if (e.getValue().getWeight() < unlabeledEdgeWeight) {
							// construct the message to send
							msg.setMsgSender(vertex.getId().get());
							msg.setSource(vertex.getId().get());
							msg.setTarget(target);
							msg.setWeight(unlabeledEdgeWeight);
							msg.setSofar(e.getValue().getWeight());
	
							// this might be a path with lower weight
							sendMessage(e.getTargetVertexId(), msg);
						}
					}
				}
			}
			
			/**every vertex processes received messages **/
			
			LRUMapWritable visitedLRUMap = vertex.getValue().getMap();

			for (SimpleEdgeWithWeight msg : messages) {
				LongWritable sourceId = new LongWritable(msg.getSource());
				LongWritable targetId = new LongWritable(msg.getTarget());
				LongLongPair msgPair = new LongLongPair(sourceId.get(), targetId.get());

				// check if this message has been received before
				if (visitedLRUMap.containsKey(msgPair)) {
					// skip this msg
				}
				else {
					// insert source and target Ids in LRU map
					visitedLRUMap.putLRU(msgPair);
					// check if this vertex is the target of the msg edge
					if (vertex.getId().get() == msg.getTarget()) {
						// check that the edge is semi-metric
						if (msg.getSofar() < vertex.getEdgeValue(sourceId).getWeight()) {
							// label the edge as semi-metric
							vertex.setEdgeValue(sourceId, new DoubleIntegerPair(msg.getSofar(), 2));
						}
					}
					else {
						// this vertex is not the target of this edge msg
						// propagate the msg to potential shorter paths
						for (Edge<LongWritable, DoubleIntegerPair> e : vertex.getEdges()) {
							// make sure not to forward the message back to its source
							// and not to forward the message back to where it came from
							if ( (e.getTargetVertexId().get() != msg.getSource()) &&
									(e.getTargetVertexId().get() != msg.getMsgSender()) ) {
								if ( (e.getValue().getWeight() + msg.getSofar()) < msg.getWeight() ) {
									msg.setSofar(e.getValue().getWeight() + msg.getSofar());
									msg.setMsgSender(vertex.getId().get());
									sendMessage(e.getTargetVertexId(), msg);
								}
							}
						}
					}
				}
			}
			// update the LRU map
			vertex.setValue(new LRUMapWritableBooleanPair(hasBeenActive, visitedLRUMap));
			
			// vertices that have already been in an active group can vote to halt
			// the rest still need to be alive in the next superstep
			if (hasBeenActive) {
				vertex.voteToHalt();
			}
		}
	}

	 /**
	   * 
	   * MasterCompute coordinates the execution.
	   *
	   */
	  public static class MasterCompute extends DefaultMasterCompute {
		    @Override
		    public void compute() {
	
		      long superstep = getSuperstep();
	
		      if (superstep == 0) {
		    	  setComputation(Initialize.class);
		      }
		      else {
		    	  setComputation(ParalleBFSCompute.class);
		      }
		  }
	  }
}