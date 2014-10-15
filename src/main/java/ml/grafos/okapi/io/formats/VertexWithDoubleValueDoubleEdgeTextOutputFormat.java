package ml.grafos.okapi.io.formats;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class VertexWithDoubleValueDoubleEdgeTextOutputFormat extends
		TextVertexOutputFormat<LongWritable, DoubleWritable, DoubleWritable> {
	
	@Override
	public TextVertexWriter createVertexWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new LongDoubleDoubleVertexWriter();
	}

	protected class LongDoubleDoubleVertexWriter extends 
			TextVertexWriterToEachLine {
		private String delimiter = "\t";

		@Override
		protected Text convertVertexToLine(
				Vertex<LongWritable, DoubleWritable, DoubleWritable> vertex)
				throws IOException {
			
			StringBuffer sb = new StringBuffer(vertex.getId().toString());
		    sb.append(delimiter);
		    sb.append(vertex.getValue().toString());
		    
			return new Text(sb.toString());
		}
	}
}
