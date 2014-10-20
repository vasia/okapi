package ml.grafos.okapi.graphs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

/**
 * This is a set of computation classes used to find 2nd-level metric edges
 * after triangles have been removed from the graph.
 * In essence, it marks edges as metric, when it is safe to make such an assumption.
 * We assume an undirected graph with symmetric edges.
 * 
 * 
 * You can run this algorithm by executing the command:
 * 
 * <pre>
 * hadoop jar $OKAPI_JAR org.apache.giraph.GiraphRunner \
 *   ml.grafos.okapi.graphs.SecondStepSemimetric\$MarkLocalMetric  \
 *   -mc  ml.grafos.okapi.graphs.SecondStepSemimetric\$MasterCompute  \
 *   -eif ml.grafos.okapi.io.formats.LongDoubleTextEdgeInputFormat  \
 *   -eip $INPUT_EDGES \
 *   -eof org.apache.giraph.io.formats.SrcIdDstIdEdgeValueTextOutputFormat \
 *   -op $OUTPUT \
 *   -w $WORKERS \
 *   -ca giraph.outEdgesClass=org.apache.giraph.edge.HashMapEdges
 *  </pre>
 * 
 */
public class SecondStepSemimetric  {

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
		double lowestWeight = Double.MAX_VALUE;
		List<LongWritable> metricEdgeTargets  = new ArrayList<LongWritable>();
		for (Edge<LongWritable, DoubleBooleanPair> e : vertex.getEdges()) {
			if (e.getValue().weight < lowestWeight) {
				lowestWeight = e.getValue().weight;
				metricEdgeTargets.clear();
				metricEdgeTargets.add(e.getTargetVertexId());
			}
			else if (e.getValue().weight == lowestWeight) {
				metricEdgeTargets.add(e.getTargetVertexId());
			}
		}
		
		// mark the lowest-weight edges as metric
		// and send a msg to the edge target vertex,
		// so that it can mark the symmetric edge as metric, too
		// if it hasn't already done so
		for (LongWritable trg : metricEdgeTargets) {
			vertex.setEdgeValue(trg, vertex.getEdgeValue(trg).setMetric(true));
			sendMessage(trg, vertex.getId());
		}
		vertex.voteToHalt();
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
			for (LongWritable msg : messages) {
				vertex.setEdgeValue(msg, vertex.getEdgeValue(msg).setMetric(true));
			}
			
			if (vertex.getNumEdges() > 1) {
				for (Edge<LongWritable, DoubleBooleanPair> e : vertex.getEdges()) {
					if(e.getValue().isMetric()) {
						double lowestWeight = Double.MAX_VALUE;
						//TODO: find a more efficient way to check this
						for (Edge<LongWritable, DoubleBooleanPair> x : vertex.getEdges()) {
							if (x.getTargetVertexId() != e.getTargetVertexId()) {
								if (x.getValue().getWeight() < lowestWeight) {
									lowestWeight  = x.getValue().getWeight();
								}
							}
						}
						sendMessage(e.getTargetVertexId(), new DoubleWritable(
						e.getValue().getWeight() + lowestWeight));
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
    	double lowestWeight = Double.MAX_VALUE;
		List<LongWritable> unlabeledTargets  = new ArrayList<LongWritable>();
		for (Edge<LongWritable, DoubleBooleanPair> e : vertex.getEdges()) {
			if (!(e.getValue().isMetric())) { // unlabeled
				if (e.getValue().weight < lowestWeight) {
					lowestWeight = e.getValue().weight;
					unlabeledTargets.clear();
					unlabeledTargets.add(e.getTargetVertexId());
				}
				else if (e.getValue().weight == lowestWeight) {
					unlabeledTargets.add(e.getTargetVertexId());
				}
			}			
		}
    	
    	// check whether their weight is smaller than all the weights received
		if (messages.iterator().hasNext()) {
			boolean isMetric = true;
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
					vertex.setEdgeValue(trg, vertex.getEdgeValue(trg).setMetric(true));
					sendMessage(trg, vertex.getId());
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
  public static class DoubleBooleanPair implements Writable {
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
      return weight + " " + metric	;
    }
  }
  
  /**
   * Use this MasterCompute implementation to find the semimetric edges.
   *
   */
  public static class MasterCompute extends DefaultMasterCompute {
    
    @Override
    public void compute() {

      long superstep = getSuperstep();  
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
//      else {
//        	// TODO: iterate steps 1 and 2 until no other metric edges can be identified
//          haltComputation();
//      }
    }
  }
}
