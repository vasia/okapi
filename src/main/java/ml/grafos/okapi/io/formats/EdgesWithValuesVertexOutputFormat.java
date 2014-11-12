package ml.grafos.okapi.io.formats;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * OutputFormat to write out the graph edges with their values,
 * one edge per line:
 *
 * <SrcVertexId><tab><TrgVertexId><tab><EdgeValue>
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public class EdgesWithValuesVertexOutputFormat<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends TextVertexOutputFormat<I, V, E> {
  /** Split delimiter */
  public static final String LINE_TOKENIZE_VALUE = "output.delimiter";
  /** Default split delimiter */
  public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

  @Override
  public EdgesWithValuesTextVertexWriter createVertexWriter(
      TaskAttemptContext context) {
    return new EdgesWithValuesTextVertexWriter();
  }

  /**
   * Vertex writer associated with {@link EdgesWithValuesVertexOutputFormat}.
   */
  protected class EdgesWithValuesTextVertexWriter extends TextVertexWriterToEachLine {
    /** Cached split delimeter */
    private String delimiter;

    @Override
    public void initialize(TaskAttemptContext context) throws IOException,
        InterruptedException {
      super.initialize(context);
      delimiter =
          getConf().get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
    }

    @Override
    public Text convertVertexToLine(Vertex<I, V, E> vertex)
      throws IOException {
      StringBuffer sb = new StringBuffer();

      for (Edge<I, E> edge : vertex.getEdges()) {
    	  sb.append(vertex.getId().toString());
    	  sb.append(delimiter).append(edge.getTargetVertexId());
    	  sb.append(delimiter).append(edge.getValue());
    	  sb.append('\n');
      }
      sb.deleteCharAt(sb.length()-1);

      return new Text(sb.toString());
    }
  }

}