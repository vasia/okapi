package ml.grafos.okapi.semimetric;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Represents the edge type for the unlabeled edges aggregator.
 * An edge with long ids and a double weight.
 */
public class UnlabeledEdge implements Writable {
	private long src;
	private long trg;
	private double w;
	
	public UnlabeledEdge() {}
	
	public UnlabeledEdge(long source, long target, double weight) {
		this.src = source;
	    this.trg = target;
	    this.w = weight;
	}

	  public long getSource() { return src; }
	  public long getTarget() { return trg; }
	  public double getWeight() { return w; }
	
	  @Override
	  public void readFields(DataInput input) throws IOException {
	  	src = input.readLong(); 
	  	trg = input.readLong();
	  	w =  input.readDouble();
	  }
	
	  @Override
	  public void write(DataOutput output) throws IOException {
	  	output.writeLong(src);
	  	output.writeLong(trg);
	  	output.writeDouble(w);
	  }
	
		public UnlabeledEdge oppositeDirectionEdge() {
			return new UnlabeledEdge(trg, src, w);
		}
}
