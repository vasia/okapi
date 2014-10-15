package ml.grafos.okapi.io.formats;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class VertexWithLongValueTextOutputFormat extends
		TextVertexOutputFormat<LongWritable, LongWritable, NullWritable> {
	
	@Override
	public TextVertexWriter createVertexWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new LongLongNullVertexWriter();
	}

	protected class LongLongNullVertexWriter extends 
			TextVertexWriterToEachLine {
		private String delimiter = "\t";

		@Override
		protected Text convertVertexToLine(
				Vertex<LongWritable, LongWritable, NullWritable> vertex)
				throws IOException {
			
			StringBuffer sb = new StringBuffer(vertex.getId().toString());
		    sb.append(delimiter);
		    sb.append(vertex.getValue().toString());
		    
			return new Text(sb.toString());
		}
	}
}
