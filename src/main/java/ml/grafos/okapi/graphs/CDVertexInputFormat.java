package ml.grafos.okapi.graphs;

import java.io.IOException;

import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class CDVertexInputFormat extends 
	TextVertexValueInputFormat<LongWritable, LongWritable, DoubleWritable> {

	@Override	
	public TextVertexValueReader createVertexValueReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new LongLongDoubleVertexValueReader();
	}
	
	  /**
	   * Vertex reader associated with
	   * {@link org.apache.giraph.io.formats.SSSPVertexValueInputFormat}.
	   */
	  
	public class LongLongDoubleVertexValueReader extends
    TextVertexValueReaderFromEachLineProcessed<Long> {

		@Override
		protected Long preprocessLine(Text line) throws IOException {
		    return Long.parseLong(line.toString());
		}

		@Override
		protected LongWritable getId(Long line) throws IOException {
			return new LongWritable(line);
		}

		@Override
		protected LongWritable getValue(Long line)
				throws IOException {
			return new LongWritable(line);
		}
		
	   }
}
