package ml.grafos.okapi.io.formats;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * The input consists for <vertexID> <vertexValue> pairs of longs
 *
 */
public class LongLongVertexValueInputFormat extends
		TextVertexValueInputFormat<LongWritable, LongWritable, NullWritable> {

	  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");
	  
		@Override
	    public LongLongVertexValueReader createVertexValueReader(
	        InputSplit split, TaskAttemptContext context) throws IOException {
	      return new LongLongVertexValueReader();
	    }

	    public class LongLongVertexValueReader extends
	    	TextVertexValueReaderFromEachLineProcessed<String[]> {

			@Override
			protected String[] preprocessLine(Text line) throws IOException {
				return SEPARATOR.split(line.toString());
			}

			@Override
			protected LongWritable getId(String[] line) throws IOException {
				return new LongWritable(Long.parseLong(line[0]));
			}

			@Override
			protected LongWritable getValue(String[] line) throws IOException {
				return new LongWritable(Long.parseLong(line[1]));
			}
	    }
}
