/**
 * Copyright 2014 Grafos.ml
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ml.grafos.okapi.semimetric.io;

import java.io.IOException;
import java.util.regex.Pattern;

import ml.grafos.okapi.semimetric.ParallelMetricBFS.DoubleIntegerPair;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Simple text-based {@link org.apache.giraph.io.EdgeInputFormat} for
 * weighted graphs with long IDs edges with double-integer pair values.
 * The boolean value of each edge is read from the input.
 *
 * Each line consists of: <source id> <target id> <edge weight> <metric-label>
 */
public class LongDoubleIntEdgeValueInputFormat extends
    TextEdgeInputFormat<LongWritable, DoubleIntegerPair> {

  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

  @Override
  public EdgeReader<LongWritable, DoubleIntegerPair> createEdgeReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new LongLongDoubleBooleanTextEdgeReader();
  }

  /**
   * {@link org.apache.giraph.io.EdgeReader} associated with
   * {@link LongLongDoubleTextEdgeInputFormat}.
   */
  public class LongLongDoubleBooleanTextEdgeReader extends
      TextEdgeReaderFromEachLineProcessed<String[]> {
    @Override
    protected String[] preprocessLine(Text line) throws IOException {
      return SEPARATOR.split(line.toString());
    }

    @Override
    protected LongWritable getSourceVertexId(String[] tokens)
      throws IOException {
      return new LongWritable(Long.parseLong(tokens[0]));
    }

    @Override
    protected LongWritable getTargetVertexId(String[] tokens)
      throws IOException {
      return new LongWritable(Long.parseLong(tokens[1]));
    }

    @Override
    protected DoubleIntegerPair getValue(String[] tokens)
      throws IOException {
    	final boolean label = Boolean.parseBoolean(tokens[3]);
    	final int intLabel = label ? 1: 3;
      return new DoubleIntegerPair(Double.parseDouble(tokens[2]), intLabel);
    }
  }
}