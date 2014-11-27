package ml.grafos.okapi.semimetric;

import junit.framework.Assert;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.AdjacencyListTextVertexOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

import ml.grafos.okapi.io.formats.VertexWithDoubleValueDoubleEdgeTextOutputFormat;
import ml.grafos.okapi.semimetric.common.TreeSetOutEdges;
import ml.grafos.okapi.semimetric.io.LongDoubleBooleanEdgeValueInputFormat;

public class TestMetricBFS {
	
	@Test
	public void testMetricBFS() throws Exception {
        String[] graph = new String[] {
        		"1	2	10.0	false", "2	1	10.0	false",
        		"1	3	2.0	true",  "3	1	2.0	true",
        		"1	6	5.0	true",  "6	1	5.0	true",
        		"1	8	6.0	true",  "8	1	6.0	true",
        		"2	5	2.0	true",  "5	2	2.0	true",
        		"3	4	3.0	true",  "4	3	3.0	true",
        		"4	5	2.0	true",  "5	4	2.0	true",
        		"4	7	4.0	true",  "7	4	4.0	true",
        		"6	7	8.0	true",  "7	6	8.0	true",
        		"8	9	2.0	true",  "9	8	2.0	true",
        		"9	10	1.0	true",  "10	9	1.0	true",
        		"9	11	1.0	true",  "11	9	1.0	true"
                 };
	      	
        // run to check results correctness
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setMasterComputeClass(MetricBFS.MasterCompute.class);
        conf.setComputationClass(MetricBFS.PutUnlabeledEdgesInAggregator.class);
        conf.setEdgeInputFormatClass(LongDoubleBooleanEdgeValueInputFormat.class);
        conf.setVertexOutputFormatClass(AdjacencyListTextVertexOutputFormat.class);
        conf.setOutEdgesClass(TreeSetOutEdges.class);

        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, null, graph);
        System.out.println("Testing metric bfs...");
        for (String s: results) {
//        	String[] output = s.split("[\t ]");
//        	if (Long.parseLong(output[0]) == 2) {
//        		Assert.assertEquals(9.0, Double.parseDouble(output[1]), 0.0001);
//        	}
        	System.out.println(s);
        }
        System.out.println();
    }    
}