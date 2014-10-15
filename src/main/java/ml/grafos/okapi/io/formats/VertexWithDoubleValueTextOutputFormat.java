package ml.grafos.okapi.io.formats;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class VertexWithDoubleValueTextOutputFormat extends
		TextVertexOutputFormat<LongWritable, DoubleWritable, FloatWritable> {
	
	@Override
	public TextVertexWriter createVertexWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new LongDoubleFloatVertexWriter();
	}

	protected class LongDoubleFloatVertexWriter extends 
			TextVertexWriterToEachLine {
		private String delimiter = "\t";

		@Override
		protected Text convertVertexToLine(
				Vertex<LongWritable, DoubleWritable, FloatWritable> vertex)
				throws IOException {
			
			StringBuffer sb = new StringBuffer(vertex.getId().toString());
		    sb.append(delimiter);
		    sb.append(vertex.getValue().toString());
		    
			return new Text(sb.toString());
		}
	}
}
