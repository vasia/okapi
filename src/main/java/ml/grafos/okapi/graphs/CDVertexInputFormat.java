package ml.grafos.okapi.graphs;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class CDVertexInputFormat extends 
	TextVertexValueInputFormat<LongWritable, LongWritable, DoubleWritable> {

	private static final Pattern SEPARATOR = Pattern.compile("[\t ]");
	 
	@Override	
	public TextVertexValueReader createVertexValueReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new LongLongDoubleVertexValueReader();
	}

	public class LongLongDoubleVertexValueReader extends
    TextVertexValueReaderFromEachLineProcessed<String> {

		@Override
		protected String preprocessLine(Text line) throws IOException {
		    return line.toString();
		}

		@Override
		protected LongWritable getId(String line) throws IOException {
			String[] tokens = SEPARATOR.split(line.toString());
			return new LongWritable(Long.parseLong(tokens[0]));
		}

		@Override
		protected LongWritable getValue(String line)
				throws IOException {
			String[] tokens = SEPARATOR.split(line.toString());
			return new LongWritable(Long.parseLong(tokens[1]));
		}
		
	   }
}
