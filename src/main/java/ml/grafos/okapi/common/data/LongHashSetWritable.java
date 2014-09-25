package ml.grafos.okapi.common.data;

import org.apache.hadoop.io.LongWritable;

public class LongHashSetWritable extends HashSetWritable<LongWritable> {

  @Override
  public void setClass() {
    setClass(LongWritable.class);
  }
}
