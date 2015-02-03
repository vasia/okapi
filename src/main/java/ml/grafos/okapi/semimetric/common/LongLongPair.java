package ml.grafos.okapi.semimetric.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 
 * A pair of two Long ids
 *
 */
public class LongLongPair implements Writable {
    long source;
    long target;

    public LongLongPair() {}

    public LongLongPair(long id1, long id2) {
      this.source = id1;
      this.target = id2;
    }

    public long getSource() { return source; }
    public long getTarget() { return target; }
    
    public void setSource (long src) {
    	this.source = src;
    }
    
    public void setTarget (long trg) {
    	this.target = trg;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
    	source = input.readLong();
    	target = input.readLong();
    }

    @Override
    public void write(DataOutput output) throws IOException {
      output.writeLong(source);
      output.writeLong(target);
    }
    
    @Override
    public String toString() {
      return source+" "+target;
    }

    @Override
	public boolean equals(Object other) {
    	LongLongPair otherPair = (LongLongPair) other;
    	return ((this.getSource() == otherPair.getSource()) &&
    			(this.getTarget() == otherPair.getTarget()));
	}
  }
