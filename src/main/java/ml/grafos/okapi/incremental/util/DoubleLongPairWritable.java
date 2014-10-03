package ml.grafos.okapi.incremental.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 
 * The type of the vertex value used for the incremental SSSP implementation.
 * The double field corresponds to the calculated shortest path distance
 * and the long corresponds to the in-SP-degree, i.e. the number of in-coming
 * SP-Edges a vertex has.
 *
 */
public class DoubleLongPairWritable implements Writable {
	  
	  private long inSPdegree;
	  private double distance;
	  
	  public DoubleLongPairWritable() {
	  }
	  
	  public DoubleLongPairWritable(double value, long degree) {
		  this.distance = value;
		  this.inSPdegree = degree;
	  }
	  
	  public double getDistance() {
		  return this.distance;
	  }
	  
	  public long getInSPdegree() {
		  return this.inSPdegree;
	  }
	  
	  public DoubleLongPairWritable setInSPdegree(long degree) {
		  this.inSPdegree = degree;
		  return this;
	  }
	  
	  public DoubleLongPairWritable decrementInSPdegree() {
		  this.inSPdegree--;
		  return this;
	  }
	  
	  public DoubleLongPairWritable setDistance(double value) {
		  this.distance = value;
		  return this;
	  }

	@Override
	public void readFields(DataInput in) throws IOException {
		distance = in.readDouble();
		inSPdegree = in.readLong();			
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(distance);
		out.writeLong(inSPdegree);			
	}
	
	@Override
	public String toString() {
		return "< " + distance + ", " + inSPdegree + " >";
	}
}