package ml.grafos.okapi.incremental.util;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Simple text-based {@link org.apache.giraph.io.EdgeInputFormat} for
 * weighted graphs with long ids, float values and boolean annotations.
 *
 * Each line consists of: <src id> <target id> <edge weight>
 * and the annotation is initialized to false.
 */
public class FloatBooleanPairEdgeInputFormat extends TextEdgeInputFormat<LongWritable, 
	FloatBooleanPairWritable> {
	 /** Splitter for endpoints */
	  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");
	  
	@Override
	public EdgeReader<LongWritable, FloatBooleanPairWritable> createEdgeReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new FloatBooleanPairTextEdgeReader();
	}
	
	 public class FloatBooleanPairTextEdgeReader extends 
	 	TextEdgeReaderFromEachLineProcessed<String[]> {

		@Override
		protected String[] preprocessLine(Text line) throws IOException {
			return SEPARATOR.split(line.toString());
		}

		@Override
		protected LongWritable getTargetVertexId(String[] line)
				throws IOException {
			return new LongWritable(Long.parseLong(line[1]));
		}

		@Override
		protected LongWritable getSourceVertexId(String[] line)
				throws IOException {
			return new LongWritable(Long.parseLong(line[0]));
		}

		@Override
		protected FloatBooleanPairWritable getValue(String[] line)
				throws IOException {
			return new FloatBooleanPairWritable(Float.parseFloat(line[2]), false);
		}
	 }

}
