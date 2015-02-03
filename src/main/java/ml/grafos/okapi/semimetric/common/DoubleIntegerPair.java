package ml.grafos.okapi.semimetric.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * Represents an edge weight together with an integer value
 * that denotes whether this edge is metric.
 *
 */
@SuppressWarnings("rawtypes")
	public class DoubleIntegerPair implements WritableComparable {
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
