package ml.grafos.okapi.incremental.util;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 
 * Vertex Input Format that reads vertex ids, distance values and in-SP-degree
 * <vertexID> <distance_from_src> <in-SP-degree>
 *
 */
public class LongDistanceAndDegreeValueInputFormat extends
	TextVertexValueInputFormat<LongWritable, DoubleLongPairWritable, FloatBooleanPairWritable> {

	private static final Pattern SEPARATOR = Pattern.compile("[\t ]");
	
	@Override
	public LongDoubleVertexValueReader createVertexValueReader(
	    InputSplit split, TaskAttemptContext context) throws IOException {
	  return new LongDoubleVertexValueReader();
	}
	
	public class LongDoubleVertexValueReader extends
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
		protected DoubleLongPairWritable getValue(String[] line) throws IOException {
			return new DoubleLongPairWritable(Double.parseDouble(line[1]), 
					Long.parseLong(line[2]));
		}
	}
}

