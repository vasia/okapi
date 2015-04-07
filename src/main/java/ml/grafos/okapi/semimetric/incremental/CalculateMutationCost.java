package ml.grafos.okapi.semimetric.incremental;

import java.io.IOException;

import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * 
 * Calculate the cost for recomputing the metric backbone, after a mutation in the original graph.
 * 
 * The program takes as input an annotated graph, where semi-metric edges are marked
 * and an event type as a configuration parameter.
 * The event type corresponds to the graph structure modification
 * and can either be an EDGE_ADDITION or an EDGE_REMOVAL.
 * The src vertex id and the trg vertex id of the edge to be added/removed
 * also needs to be specified as a configuration parameter.
 * 
 * The output is the number of BFSs that will be triggered by the mutation event.
 * 
 * 1. EDGE_REMOVAL: 
 * - If the removed edge is semi-metric, no computation is needed. The output is 0.
 * - If the removed edge is metric, we need to find all alternative paths
 * between the edge endpoints and count the number of semi-metric edges encountered
 * in all of these paths. The output would then be #(semi-metric edges found) + 1.
 *  
 * 2. EDGE_ADDITION:
 * - We need to execute one BFS to find out of the edge to be added is semi-metric.
 * -- if it is semi-metric, no further computation is required --> output 1.
 * -- if it is metric, we need to find all alternative paths
 * between the edge endpoints and count the number of metric edges encountered
 * in all of these paths. The output would then be #(metric edges found) + 1.
 */

public class CalculateMutationCost {
	
	/** The event type that triggers the incremental computation.
	 * It can take two values:  1 for EDGE_ADDITION or 2 for EDGE_REMOVAL */
	 private static final String EVENT_TYPE = "event.type";
	 
	 /** The source vertex id of the edge to be added/removed */
	 private static final String EVENT_EDGE_SRC = "event.edge.src";
	 
	 /** The target vertex id of the edge to be added/removed */
	 private static final String EVENT_EDGE_TRG = "event.edge.trg";
	 
	 private static final int EDGE_ADDITION = 1;
	 
	 private static final int EDGE_REMOVAL = 2;
	 
	 /** The BFSs aggregator*/
	 public static final String BFS_AGGREGATOR = "bfs.aggregator";

	 /**
	  * Checks whether the edge to remove is semi-metric.
	  */
	 public static class CheckSemiMetric extends BasicComputation<LongWritable, BooleanWritable, 
	 	BooleanWritable, Text> {

		private long edgeSrc;
		private long edgeTrg;
		 
		@Override
		public void preSuperstep() {
			edgeSrc = getContext().getConfiguration().getLong(EVENT_EDGE_SRC, -1L);
			edgeTrg = getContext().getConfiguration().getLong(EVENT_EDGE_TRG, -1L);
		};
		
		@Override
		public void compute(
				Vertex<LongWritable, BooleanWritable, BooleanWritable> vertex, 
				Iterable<Text> messages) throws IOException {

			LongWritable trgVertexId = new LongWritable(edgeTrg);
			
			if (vertex.getId().get() == edgeSrc) {
				// the edge is semi-metric if it has a "true" label
				if (vertex.getEdgeValue(trgVertexId).get()) {
					// no computation is required => set the aggregator to 0
					aggregate(BFS_AGGREGATOR, new IntWritable(0));
				}
				else {
					// the edge is metric -> remove it
					vertex.removeEdges(trgVertexId);
					// we need to find all semi-metric edges in alternative paths
					// => set the aggregator to 1
					aggregate(BFS_AGGREGATOR, new IntWritable(1));
				}
			}
			// set all vertices to "not visited"
			vertex.setValue(new BooleanWritable(false));
		}
	 }

	 /**
	  * 
	  * Executes a BFS step from the src of the edge to be removed to the target
	  * and accumulates the paths found on the way.
	  * 
	  * If a vertex receives a path sequence containing its own ID, 
	  * then it stops the propagation (cycle).
	  * 
	  * At the end of the BFS, 
	  * either the target of the edge will have received all the alternative paths
	  * or none, in which case no such paths exist.
	  * 
	  */
	 public static class FindAlternativePaths extends BasicComputation<LongWritable, BooleanWritable, 
	 	BooleanWritable, Text> {

		private long edgeSrc;
		private long edgeTrg;
		 
		@Override
		public void preSuperstep() {
			edgeSrc = getContext().getConfiguration().getLong(EVENT_EDGE_SRC, -1L);
			edgeTrg = getContext().getConfiguration().getLong(EVENT_EDGE_TRG, -1L);
		};
			
		@Override
		public void compute(Vertex<LongWritable, BooleanWritable, BooleanWritable> vertex,
				Iterable<Text> messages) throws IOException {
			
			if (getSuperstep() == 1) {
				// only the source vertex propagates a message
				if (vertex.getId().get() == edgeSrc) {
					Text msg = new Text(vertex.getId().toString());
					for (Edge<LongWritable, BooleanWritable> e : vertex.getEdges()) {
						sendMessage(e.getTargetVertexId(), msg);
					}
				}
				vertex.voteToHalt();
			}
			else {
				// if the receiving vertex is the target => print the message
				// TODO: instead of printing, put in a HashSet aggregator
				if (vertex.getId().get() == edgeTrg) {
					for (Text path : messages) {
						System.out.println("Path " + path.toString());
					}
					vertex.voteToHalt();
				}
				else {
					// check if the received message contains own ID
					// if yes, don't propagate
					// otherwise, add own ID and send to all neighbors
					for (Text msg : messages) {
						String msgString = msg.toString();
						String vertexId = vertex.getId().toString();
						if (msgString.contains(vertexId)) {
							// cycle detected
						}
						else {
							Text pathToSend = new Text(msgString.concat("\t" + vertexId)); 
							// propagate to all neighbors
							for (Edge<LongWritable, BooleanWritable> e : vertex.getEdges()) {
								sendMessage(e.getTargetVertexId(), pathToSend);
							}
						}
					}
					vertex.voteToHalt();
				}
			}
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
			  
			  // register the BFS aggregator
			  registerPersistentAggregator(BFS_AGGREGATOR, IntSumAggregator.class);
		  }
		  
		  @Override
		  public final void compute() {
			  
		    long superstep = getSuperstep();

		    if (eventType == EDGE_REMOVAL) {
			    if (superstep == 0) {
			    	setComputation(CheckSemiMetric.class);
				} else if (superstep == 1) {
					// check the aggregator value: if it is 0, 
					// the edge is semi-metric => print and halt computation
					// if it is 1 => find all semi-metric edges in alternative paths
					int aggrValue = ((IntWritable)getAggregatedValue(BFS_AGGREGATOR)).get();
					if (aggrValue == 0) {
						System.out.println("Number of BFSs triggered: 0");
						haltComputation();
					}
					else if (aggrValue == 1) {
						setComputation(FindAlternativePaths.class);
					}
					else {
				    	System.err.print("Invalid Aggregator value");
				    	System.exit(-1);						
					}
				} else {
					// superstep > 1
					setComputation(FindAlternativePaths.class);
				}
		    } else if (eventType == EDGE_ADDITION) {
		    	
		    } else {
		    	System.err.print("Unkown event type");
		    	System.exit(-1);
		    }
		  }
	  }
}
