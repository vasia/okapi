package ml.grafos.okapi.graphs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Set;

import ml.grafos.okapi.clustering.kmeans.ArrayListOfDoubleArrayListWritableAggregator;
import ml.grafos.okapi.clustering.kmeans.KMeansClustering;
import ml.grafos.okapi.clustering.kmeans.KMeansTextInputFormat;
import ml.grafos.okapi.clustering.kmeans.KMeansTextOutputFormat;
import ml.grafos.okapi.clustering.kmeans.KMeansClustering.KMeansClusteringComputation;
import ml.grafos.okapi.common.data.ArrayListOfDoubleArrayListWritable;
import ml.grafos.okapi.common.data.DoubleArrayListWritable;
import ml.grafos.okapi.common.graph.NullOutEdges;
import ml.grafos.okapi.io.formats.LongDoubleBooleanEdgeInputFormat;

import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.HashMapEdges;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.AdjacencyListTextVertexOutputFormat;
import org.apache.giraph.io.formats.SrcIdDstIdEdgeValueTextOutputFormat;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.Test;

import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;

public class TestSecondStepSemimetric {
	
	@Test
	public void test() throws Exception {
        String[] graph = new String[] {
        		"1	2	3.0",
        		"1	3	2.0",
        		"2	4	1.0",
        		"3	4	2.0",
        		"4	5	5.0",
        		"4	6	3.0",
        		"4	7	3.0",
        		"2	1	3.0",
        		"3	1	2.0",
        		"4	2	1.0",
        		"4	3	2.0",
        		"5	4	5.0",
        		"6	4	3.0",
        		"7	4	3.0",
        		"7	8	1.0",
        		"8	7	1.0"
                 };
	      	
        // run to check results correctness
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setMasterComputeClass(SecondStepSemimetric.MasterCompute.class);
        conf.setComputationClass(SecondStepSemimetric.MarkLocalMetric.class);
        conf.setEdgeInputFormatClass(LongDoubleBooleanEdgeInputFormat.class);
        conf.setVertexOutputFormatClass(AdjacencyListTextVertexOutputFormat.class);
        //AVK: edge output format doesn't work
//        conf.setEdgeOutputFormatClass(SrcIdDstIdEdgeValueTextOutputFormat.class);
        conf.setOutEdgesClass(HashMapEdges.class);

        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, null, graph);
        for (String s: results) {
        	System.out.println(s);
        }
    }
    
}