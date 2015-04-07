package ml.grafos.okapi.semimetric.incremental;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Represents the edge type for the HashSet edges aggregator.
 * Essentially a pair of Long ids.
 */
public class EdgeIdsPair implements Writable {
	private long src;
	private long trg;
	
	public EdgeIdsPair() {}
	
	public EdgeIdsPair(long source, long target) {
		this.src = source;
	    this.trg = target;
	}

	  public long getSource() { return src; }
	  public long getTarget() { return trg; }

	  public void setSource(long source) {
		  this.src = source;
	  }
	  
	  public void setTarget(long target) {
		  this.trg = target;
	  }
	
	  @Override
	  public void readFields(DataInput input) throws IOException {
	  	src = input.readLong(); 
	  	trg = input.readLong();
	  }
	
	  @Override
	  public void write(DataOutput output) throws IOException {
	  	output.writeLong(src);
	  	output.writeLong(trg);
	  }
		
		public void set(EdgeIdsPair other) {
			this.src = other.getSource();
			this.trg = other.getTarget();
		}
		
		@Override
		public boolean equals(Object other) {
			EdgeIdsPair otherEdge = (EdgeIdsPair)other;
			return ((otherEdge.getSource() == this.getSource()) 
					&& (otherEdge.getTarget() == this.getTarget()));
		}
}
