package ml.grafos.okapi.incremental.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class LongWithDoubleWritable implements Writable {
	  
	  private LongWritable vertexID;
	  private DoubleWritable vertexValue;
	  
	  public LongWithDoubleWritable() {
		  this.vertexID = new LongWritable();
		  this.vertexValue = new DoubleWritable();
	  }
	  
	  public LongWithDoubleWritable(LongWritable id, DoubleWritable value) {
		  this.vertexID = id;
		  this.vertexValue = value;
	  }
	  
	  public LongWritable getVertexId() {
		  return this.vertexID;
	  }
	  
	  public DoubleWritable getVertexValue() {
		  return this.vertexValue;
	  }

	@Override
	public void readFields(DataInput in) throws IOException {
		vertexID.readFields(in);
		vertexValue.readFields(in);			
	}

	@Override
	public void write(DataOutput out) throws IOException {
		vertexID.write(out);
		vertexValue.write(out);			
	}
}