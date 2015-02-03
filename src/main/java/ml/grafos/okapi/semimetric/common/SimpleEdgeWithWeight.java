package ml.grafos.okapi.semimetric.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Represents an undirected edge with a symmetric weight
 * and the discovered weight so far.
 *
 */
public class SimpleEdgeWithWeight implements Writable {

		long msgSender;
		long source;
		long target;
		double weight;
		double sofar;

  public SimpleEdgeWithWeight() {}

  public SimpleEdgeWithWeight(long sender, long id1, long id2, double weight, double sofar) {
  	this.msgSender = sender;
  	this.source = id1;
  	this.target = id2;
  	this.weight = weight;
  	this.sofar = sofar;
  }

  public long getMsgSender() { return msgSender; }
  public long getSource() { return source; }
  public long getTarget() { return target; }
  public double getWeight() { return weight; }
  public double getSofar() { return sofar; }
  
  public void setMsgSender (long sender) {
  	this.msgSender = sender;
  }
  
  public void setSource (long src) {
  	this.source = src;
  }
  
  public void setTarget (long trg) {
  	this.target = trg;
  }
  
  public void setWeight (double w) {
  	this.weight = w;
  }
  
  public void setSofar (double sf) {
  	this.sofar = sf;
  }


  @Override
  public void readFields(DataInput input) throws IOException {
  	msgSender = input.readLong();
  	source = input.readLong();
  	target = input.readLong();
  	weight = input.readDouble();
  	sofar = input.readDouble();
  }

  @Override
  public void write(DataOutput output) throws IOException {
  	output.writeLong(msgSender);
  	output.writeLong(source);
  	output.writeLong(target);
  	output.writeDouble(weight);
  	output.writeDouble(sofar);
  }
  
  @Override
  public String toString() {
    return msgSender + " " +source+" "+target+" "+weight + " "+sofar;
  }
}
