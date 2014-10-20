package ml.grafos.okapi.graphs;

import ml.grafos.okapi.io.formats.LongDoubleBooleanEdgeInputFormat;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.HashMapEdges;
import org.apache.giraph.io.formats.AdjacencyListTextVertexOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

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