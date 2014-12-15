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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Implementation of the last step of the backbone algorithm.
 * The input is a graph with some labeled and some unlabeled edges
 * For every unlabeled edge (u, v), u propagates a message to its neighbors
 * to explore paths that have weight lower than the weight of (u, v).
 * 
 * At the end of the computation, a previously unlabeled edge 
 * has either been marked as semi-metric or is still unlabeled, which means it is metric. 
 */
public class ParallelMetricBFS {

	/**
	 * During the first superstep, every vertex gathers its unlabeled edges.
	 * For each unlabeled edge, it creates a message and propagates it
	 * along all edges that have weight lower than the unlabeled edge weight. 
	 */
	public static class PropagateUnlabeledEdges extends BasicComputation<LongWritable, 
		NullWritable, DoubleIntegerPair, SimpleEdgeWithWeight> {

		@Override
		public void compute(Vertex<LongWritable, NullWritable, DoubleIntegerPair> vertex,
				Iterable<SimpleEdgeWithWeight> messages) throws IOException {
			
			// retrieve list of unlabeled edges
			List<Edge<LongWritable, DoubleIntegerPair>> unlabeledEdges = new ArrayList<Edge<LongWritable, DoubleIntegerPair>>();

			for (Edge<LongWritable, DoubleIntegerPair> e : vertex.getEdges()) {
				if (!(e.getValue().isMetric())) {
					unlabeledEdges.add(EdgeFactory.create(e.getTargetVertexId(), e.getValue()));
				}
			}

			// for each unlabeled edge, send a message to all other edges with weight
			// less than the weight of this edge (possible shorter paths)
			for (Edge<LongWritable, DoubleIntegerPair> unlabeledEdge : unlabeledEdges) {

				for (Edge<LongWritable, DoubleIntegerPair> e : vertex.getEdges()) {
					if (e.getValue().getWeight() < unlabeledEdge.getValue().getWeight()) {
						// construct the message to send
						SimpleEdgeWithWeight msg  = new SimpleEdgeWithWeight(vertex.getId().get(), unlabeledEdge.getTargetVertexId().get(), 
								unlabeledEdge.getValue().getWeight(), e.getValue().getWeight());

						// this might be a path with lower weight
						sendMessage(e.getTargetVertexId(), msg);
					}
				}
			}
			vertex.voteToHalt();
		}
	}
	
	/**
	 * Computation class for the custom BFS.
	 * Upon receiving a msg, a vertex checks whether it is the target of the msg edge: 
	 * - if yes, the msg edge is semi-metric.
	 * - otherwise, it propagates the msg to edges that can lead to shortest paths.
	 */
	public static class CustomBFS extends BasicComputation<LongWritable, 
	NullWritable, DoubleIntegerPair, SimpleEdgeWithWeight> {

		@Override
		public void compute(
				Vertex<LongWritable, NullWritable, DoubleIntegerPair> vertex,
				Iterable<SimpleEdgeWithWeight> messages) throws IOException {
			
			for (SimpleEdgeWithWeight msg : messages) {
				// check if this vertex is the target of the msg edge
				if (vertex.getId().get() == msg.getTarget()) {
					// check that the edge is semi-metric
					LongWritable sourceId = new LongWritable(msg.getSource());
					if (msg.getSofar() < vertex.getEdgeValue(sourceId).getWeight()) {
						// label the edge as semi-metric
						vertex.setEdgeValue(sourceId, new DoubleIntegerPair(msg.getSofar(), 2));
					}
				}
				else {
					// this vertex is not the target of this edge msg
					// propagate the msg to potential shorter paths
					for (Edge<LongWritable, DoubleIntegerPair> e : vertex.getEdges()) {
						if ( (e.getValue().getWeight() + msg.getSofar()) < msg.getWeight() ) {
							SimpleEdgeWithWeight msgToSend  = 
									new SimpleEdgeWithWeight(msg.getSource(), msg.getTarget(),
											msg.getWeight(), e.getValue().getWeight() + msg.getSofar());
							sendMessage(e.getTargetVertexId(), msgToSend);
						}
					}
				}
			}
			vertex.voteToHalt();
		}
	}

	 /**
	   * 
	   * MastercCompute coordinates the execution.
	   *
	   */
	  public static class MasterCompute extends DefaultMasterCompute {
		    @Override
		    public void compute() {
	
		      long superstep = getSuperstep();
	
		      if (superstep == 0) {
		    	  setComputation(PropagateUnlabeledEdges.class);
		      }
		      else {
		    	  setComputation(CustomBFS.class);
		      }
		  }
	  }

	  /**
	   * Represents an undirected edge with a symmetric weight
	   * and the discovered weight so far.
	   *
	   */
	  public static class SimpleEdgeWithWeight implements Writable {
	    long source;
	    long target;
	    double weight;
	    double sofar;

	    public SimpleEdgeWithWeight() {}

	    public SimpleEdgeWithWeight(long id1, long id2, double weight, double sofar) {
	      this.source = id1;
	      this.target = id2;
	      this.weight = weight;
	      this.sofar = sofar;
	    }

	    public long getSource() { return source; }
	    public long getTarget() { return target; }
	    public double getWeight() { return weight; }
	    public double getSofar() { return sofar; }

	    @Override
	    public void readFields(DataInput input) throws IOException {
	    	source = input.readLong();
	    	target = input.readLong();
	      weight = input.readDouble();
	      sofar = input.readDouble();
	    }

	    @Override
	    public void write(DataOutput output) throws IOException {
	      output.writeLong(source);
	      output.writeLong(target);
	      output.writeDouble(weight);
	      output.writeDouble(sofar);
	    }
	    
	    @Override
	    public String toString() {
	      return source+" "+target+" "+weight + " "+sofar;
	    }
	  }
	  
	  /**
	   * Represents an edge weight together with an integer value
	   * that denotes whether this edge is metric.
	   *
	   */
	  @SuppressWarnings("rawtypes")
		public static class DoubleIntegerPair implements WritableComparable {
		  /**
		   * 1: metric
		   * 2: semi-metric
		   * 3: unlabeled
		   */
		    double weight;
		    int metric = 3;
	
		    public DoubleIntegerPair() {}
	
		    public DoubleIntegerPair(double weight, int metric) {
		      this.weight = weight;
		      this.metric = metric;
		    }
	
		    public double getWeight() { return weight; }
		    public boolean isMetric() { return metric == 1; }
		    
		    public DoubleIntegerPair setMetricLabel(int value) {
		    	this.metric = value; 
		    	return this;
		    }
	
		    @Override
		    public void readFields(DataInput input) throws IOException {
		      weight = input.readDouble();
		      metric = input.readInt();
		    }
	
		    @Override
		    public void write(DataOutput output) throws IOException {
		      output.writeDouble(weight);
		      output.writeInt(metric);
		    }
		    
		    @Override
		    public String toString() {
		      return weight + "\t" + metric	;
		    }
	
			@Override
			public int compareTo(Object other) {
				DoubleIntegerPair otherPair = (DoubleIntegerPair) other;
				if (this.getWeight() < otherPair.getWeight()) {
					return -1;
				}
				else if (this.getWeight() > otherPair.getWeight()) {
					return 1;
				}
				else {
					return 0;
				}
			}
		  }
}