package ml.grafos.okapi.common.data;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

public class LongHashSetWritableTest {

  @Test
  public void test1() throws IOException {
    LongHashSetWritable s1 = new LongHashSetWritable();
    s1.add(new LongWritable(1));
    s1.add(new LongWritable(2));
    s1.add(new LongWritable(3));

    ByteArrayOutputStream baos = new ByteArrayOutputStream(10000);
    DataOutput output = new DataOutputStream(baos);
    s1.write(output);

    LongHashSetWritable copy = new LongHashSetWritable();
    DataInputStream input = new DataInputStream(new ByteArrayInputStream(
        baos.toByteArray()));
    copy.readFields(input);
    assertTrue(copy.equals(s1));
  }
}

