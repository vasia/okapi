package ml.grafos.okapi.graphs;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.IntNullTextEdgeInputFormat;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class CDEdgeInputFormat extends TextEdgeInputFormat<LongWritable, DoubleWritable> {
	
	 /** Splitter for endpoints */
	  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

	  @Override
	  public EdgeReader<LongWritable, DoubleWritable> createEdgeReader(
	      InputSplit split, TaskAttemptContext context) throws IOException {
	    return new LongDoubleTextEdgeReader();
	  }

	  /**
	   * {@link org.apache.giraph.io.EdgeReader} associated with
	   * {@link IntNullTextEdgeInputFormat}.
	   */
	  public class LongDoubleTextEdgeReader extends
	      TextEdgeReaderFromEachLineProcessed<EdgeWithDoubleValue> {

		@Override
		protected EdgeWithDoubleValue preprocessLine(Text line)
				throws IOException {
			String[] tokens = SEPARATOR.split(line.toString());
			return new EdgeWithDoubleValue(Long.valueOf(tokens[0]), Long.valueOf(tokens[1]), 
					Float.valueOf(tokens[2]));
		}

		@Override
		protected LongWritable getTargetVertexId(EdgeWithDoubleValue line)
				throws IOException {
			return new LongWritable(line.getTrg());
		}

		@Override
		protected LongWritable getSourceVertexId(EdgeWithDoubleValue line)
				throws IOException {
			return new LongWritable(line.getSrc());
		}

		@Override
		protected DoubleWritable getValue(EdgeWithDoubleValue line)
				throws IOException {
			return new DoubleWritable(line.getValue());
		}
		  
	   
	  }
	  
	  public class EdgeWithDoubleValue {
		  private long src;
		  private long trg;
		  private double value;
		  
		  public EdgeWithDoubleValue(long first, long second, double weight) {
			  this.src = first;
			  this.trg = second;
			  this.value = weight;
			  }
		  
		  public void setSrc(long first) {
			  this.src = first;
		  }
		  
		  public void setTrg(long second) {
			  this.trg = second;
		  }
		  
		  public void setValue(double weight) {
			  this.value = weight;
		  }
		  public long getSrc() {
			  return this.src;
		  }
		  public long getTrg() {
			  return this.trg;
		  }
		  public double getValue() {
			  return this.value;
		  }
		  
		  
	  }
}
