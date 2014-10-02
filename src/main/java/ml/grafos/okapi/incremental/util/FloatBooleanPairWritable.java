package ml.grafos.okapi.incremental.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FloatBooleanPairWritable implements Writable {

	  private float weight;
	  private boolean isSPEdge;
	  
	  public FloatBooleanPairWritable() {}
	  
	  public FloatBooleanPairWritable(float weight, boolean isSPEdge) {
		this.weight = weight;
		this.isSPEdge = isSPEdge;
	  }
	  
	  public boolean belongsToSPG() {
		  return this.isSPEdge;
	  }
	  
	  public FloatBooleanPairWritable setSPEdge(boolean isSPEdge) {
		  this.isSPEdge = isSPEdge;
		  return this;
	  }
	  
	  public float getWeight() {
		  return this.weight;
	  }
	  
	  @Override
	  public void readFields(DataInput in) throws IOException {
		weight = in.readFloat();
		isSPEdge = in.readBoolean();
	  }
	
	  @Override
	  public void write(DataOutput out) throws IOException {
		out.writeFloat(weight);
		out.writeBoolean(isSPEdge);			
	  }
	  
	  @Override
	  public String toString() {
		  if (belongsToSPG()) {
			  return "< " + weight + ", SPEdge >";
		  }
		  else {
			  return "< " + weight + " >";
		  }
	  }
}
