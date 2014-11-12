package ml.grafos.okapi.semimetric;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * This is a set of computation classes used to find 2nd-level metric edges
 * after triangles have been removed from the graph.
 * In essence, it marks edges as metric, when it is safe to make such an assumption.
 * We assume an undirected graph with symmetric edges.
 * 
 * IMPORTANT NOTE: This implementation assumes that the type of the OutEdges
 * is TreeSetOutEdges.
 * 
 * 
 * You can run this algorithm by executing the command:
 * 
 * <pre>
 * hadoop jar $OKAPI_JAR org.apache.giraph.GiraphRunner \
 *   ml.grafos.okapi.graphs.SecondStepSemimetric\$MarkLocalMetric  \
 *   -mc  ml.grafos.okapi.graphs.SecondStepSemimetric\$MasterCompute  \
 *   -eif ml.grafos.okapi.io.formats.LongDoubleBooleanEdgeInputFormat  \
 *   -eip $INPUT_EDGES \
 *   -eof org.apache.giraph.io.formats.SrcIdDstIdEdgeValueTextOutputFormat \
 *   -op $OUTPUT \
 *   -w $WORKERS \
 *   -ca giraph.outEdgesClass=org.apache.giraph.edge.TreeSetOutEdges
 *  </pre>
 * 
 */
public class SecondStepSemimetric  {
	
	/** The metric edges aggregator*/
	public static final String METRIC_EDGES_AGGREGATOR = "metric.edges.aggregator";

	/**
	 * This class implements the first step:
	 * Each node marks as metric its edge(s) with the lowest weight.
	 * Then, it sends a msg with its id and the edge weight,
	 * along the edge(s) with the lowest weight.
	 *
	 */
	public static class MarkLocalMetric extends BasicComputation<LongWritable,
  		NullWritable, DoubleBooleanPair, LongWritable> {

		@Override
		public void compute(
			Vertex<LongWritable, NullWritable, DoubleBooleanPair> vertex,
			Iterable<LongWritable> messages) throws IOException {
	
			// find the edge(s) with the lowest weight
			List<LongWritable> metricEdgeTargets  = new ArrayList<LongWritable>();
			Iterator<Edge<LongWritable, DoubleBooleanPair>> edgeIterator = 
					vertex.getEdges().iterator();
			if (edgeIterator.hasNext()) {
				Edge<LongWritable, DoubleBooleanPair> first = edgeIterator.next(); 
				double lowestWeight = first.getValue().getWeight();
				metricEdgeTargets.add(first.getTargetVertexId());
				
				while (edgeIterator.hasNext()) {
					Edge<LongWritable, DoubleBooleanPair> edge = edgeIterator.next();
					assert(edge.getValue().getWeight() >= lowestWeight);
					if (edge.getValue().getWeight() > lowestWeight) {
						break;
					}
					else {
						metricEdgeTargets.add(edge.getTargetVertexId());
					}
				}
			}

			// mark the lowest-weight edges as metric
			// and send a msg to the edge target vertex,
			// so that it can mark the symmetric edge as metric, too
			// if it hasn't already done so
			for (LongWritable trg : metricEdgeTargets) {
				vertex.setEdgeValue(trg, vertex.getEdgeValue(trg).setMetric(true));
				sendMessage(trg, vertex.getId());
				aggregate(METRIC_EDGES_AGGREGATOR, new LongWritable(1));
			}
		}
	}
  
  /**
   * This class implements the second step:
   * First, each vertex processes the received messages and marks
   * all the metric edges that were discovered by neighbors.
   * Then, it sends a msg to all its metric edges, with the sum of the weight
   * of this edge, plus the smallest among the weights of the rest of its edges
   *
   */
  public static class SendMsgToMetricEdge extends AbstractComputation<LongWritable,
	NullWritable, DoubleBooleanPair, LongWritable, DoubleWritable> {
	  
	@Override
	public void compute(
			Vertex<LongWritable, NullWritable, DoubleBooleanPair> vertex,
			Iterable<LongWritable> messages) throws IOException {
		
			// receive msgs containing potentially new metric edges
			boolean newMetricEdge = false;
			for (LongWritable msg : messages) {
				if (!(vertex.getEdgeValue(msg).isMetric())) {
					vertex.setEdgeValue(msg, vertex.getEdgeValue(msg).setMetric(true));
					newMetricEdge = true;
					aggregate(METRIC_EDGES_AGGREGATOR, new LongWritable(1));
				}
			}
			
			// the first time this is executed, there is no need to check whether new metric edges
			// were discovered
			double weightToSend;
			if ((getSuperstep() == 1) || (newMetricEdge)) {
				if (vertex.getNumEdges() > 1) {
					Iterator<Edge<LongWritable, DoubleBooleanPair>> edges = vertex.getEdges().iterator();
					if (edges.hasNext()) {
						// the first msg is the weight of the first two edges
						Edge<LongWritable, DoubleBooleanPair> first = edges.next();
						weightToSend = first.getValue().getWeight();
						if (edges.hasNext()) {
							sendMessage(first.getTargetVertexId(), new DoubleWritable(weightToSend
									+ edges.next().getValue().getWeight()));
						}
						
						// for the rest of the messages, it is the weight of the current edge
						// plus the weight of the first edge (guaranteed to be the smallest)
						while(edges.hasNext()) {
							Edge<LongWritable, DoubleBooleanPair> e = edges.next();
							if(e.getValue().isMetric()) {
								weightToSend = first.getValue().getWeight() + e.getValue().getWeight();
								sendMessage(e.getTargetVertexId(), new DoubleWritable(weightToSend));
							}
							else {
								// we don't need to check any further for metric edges
								// if we encountered an unlabeled edge, then all the following edges
								// are also unlabeled
								break; 
							}
						}
					}
				}
			}
			vertex.voteToHalt();
		}
  	}
  
  
  /**
   * This class implements the last step:
   * Each node checks whether it can reason about the semimetricity
   * of its smallest-weight unlabeled edge.
   * If all of the weights in the received messages are larger
   * than the weight of this edge, then this edge can be safely marked as metric.
   * If the edge is marked as metric, send a message to its target vertex,
   * which might have not discovered this yet. 
   *
   */
public static class CheckSmallestUnlabeledEdge extends AbstractComputation<LongWritable, 
	NullWritable, DoubleBooleanPair, DoubleWritable, LongWritable> {

    @Override
    public void compute(Vertex<LongWritable, NullWritable, DoubleBooleanPair> vertex, 
        Iterable<DoubleWritable> messages) throws IOException {
    	
    	// find smallest-weight unlabeled edge(s)
    	List<LongWritable> unlabeledTargets  = new ArrayList<LongWritable>();
		Iterator<Edge<LongWritable, DoubleBooleanPair>> edgeIterator = 
				vertex.getEdges().iterator();
		double lowestWeight = Double.MAX_VALUE;
		
		if (edgeIterator.hasNext()) {
			Edge<LongWritable, DoubleBooleanPair> edge = edgeIterator.next();
			while ((edge.getValue().isMetric()) && (edgeIterator.hasNext())) {
				edge = edgeIterator.next();
			}
				
			if (!(edge.getValue().isMetric())) {
				// first encountered unlabeled edge
				lowestWeight = edge.getValue().getWeight();
				unlabeledTargets.add(edge.getTargetVertexId());
				
				// check if there are more with the same weight
				while (edgeIterator.hasNext()) {
					Edge<LongWritable, DoubleBooleanPair> nextEdge = edgeIterator.next();
					assert(nextEdge.getValue().getWeight() >= lowestWeight);
					if (nextEdge.getValue().getWeight() > lowestWeight) {
						break;
					}
					else {
						unlabeledTargets.add(nextEdge.getTargetVertexId());
					}
				}
			}
		}
    	
    	// check whether their weight is smaller than all the weights received
		boolean isMetric = messages.iterator().hasNext();
		for (DoubleWritable msg : messages) {
			if (msg.get() < lowestWeight) {
				isMetric = false;
				break;
			}
		}
			// label the metric edges, if any found
			// and let the neighbors know
			if (isMetric) {
				for (LongWritable trg : unlabeledTargets) {
					if (!(vertex.getEdgeValue(trg).isMetric())) {
						vertex.setEdgeValue(trg, vertex.getEdgeValue(trg).setMetric(true));
						sendMessage(trg, vertex.getId());
						aggregate(METRIC_EDGES_AGGREGATOR, new LongWritable(1));
					}
				}
			}
	
		vertex.voteToHalt();
    }
  }

  /**
   * Represents an edge weight together with a boolean value
   * that denotes whether this edge is metric.
   *
   */
  @SuppressWarnings("rawtypes")
public static class DoubleBooleanPair implements WritableComparable {
    double weight;
    boolean metric = false;

    public DoubleBooleanPair() {}

    public DoubleBooleanPair(double weight, boolean metric) {
      this.weight = weight;
      this.metric = metric;
    }

    public double getWeight() { return weight; }
    public boolean isMetric() { return metric; }
    
    public DoubleBooleanPair setMetric(boolean value) {
    	this.metric = value; 
    	return this;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
      weight = input.readDouble();
      metric = input.readBoolean();
    }

    @Override
    public void write(DataOutput output) throws IOException {
      output.writeDouble(weight);
      output.writeBoolean(metric);
    }
    
    @Override
    public String toString() {
      return weight + "\t" + metric	;
    }

	@Override
	public int compareTo(Object other) {
		DoubleBooleanPair otherPair = (DoubleBooleanPair) other;
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
  
  /**
   * Use this MasterCompute implementation to find the semimetric edges.
   *
   */
  public static class MasterCompute extends DefaultMasterCompute {
    
	@Override
    public final void initialize() throws InstantiationException,
        IllegalAccessException {
    	// register metric edges aggregator
    	registerPersistentAggregator(METRIC_EDGES_AGGREGATOR, LongSumAggregator.class);
	}
	  
    @Override
    public void compute() {

      long superstep = getSuperstep();
      System.out.println("*** superstep: " + superstep +", metric edges: " + getAggregatedValue(METRIC_EDGES_AGGREGATOR));
      if (superstep==0) {
    	  setComputation(MarkLocalMetric.class);
      } else if ((superstep % 2) != 0){
    	  setComputation(SendMsgToMetricEdge.class);
    	  setIncomingMessage(LongWritable.class);
    	  setOutgoingMessage(DoubleWritable.class);  
      } else {
	      setComputation(CheckSmallestUnlabeledEdge.class);
	      setIncomingMessage(DoubleWritable.class);
	      setOutgoingMessage(LongWritable.class);
      }
    }
  }
}
