package ml.grafos.okapi.graphs;

import junit.framework.Assert;
import ml.grafos.okapi.common.edge.TreeSetOutEdges;
import ml.grafos.okapi.io.formats.LongDoubleBooleanEdgeInputFormat;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

import ml.grafos.okapi.io.formats.EdgesWithValuesVertexOutputFormat;

public class TestSecondStepSemimetric {
	
	@Test
	public void testAllMetric() throws Exception {
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
        conf.setVertexOutputFormatClass(EdgesWithValuesVertexOutputFormat.class);
        conf.setOutEdgesClass(TreeSetOutEdges.class);

        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, null, graph);
        System.out.println("Testing all-metric...");
        for (String s: results) {
        	Assert.assertEquals(true, Boolean.parseBoolean(s.split("[\t ]")[3]));
        	System.out.println(s);
        }
        System.out.println();
    }
	
	@Test
	public void testOneSemimetric() throws Exception {
        String[] graph = new String[] {
        		"1	2	1.0",
        		"2	1	1.0",
        		"2	3	2.0",
        		"3	2	2.0",
        		"3	4	3.0",
        		"4	3	3.0",
        		"4	1	7.0",
        		"1	4	7.0",
                 };
	      	
        // run to check results correctness
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setMasterComputeClass(SecondStepSemimetric.MasterCompute.class);
        conf.setComputationClass(SecondStepSemimetric.MarkLocalMetric.class);
        conf.setEdgeInputFormatClass(LongDoubleBooleanEdgeInputFormat.class);
        conf.setVertexOutputFormatClass(EdgesWithValuesVertexOutputFormat.class);
        conf.setOutEdgesClass(TreeSetOutEdges.class);

        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, null, graph);
        System.out.println("Testing one-semimetric...");
        for (String s: results) {
        	String[] tokens = s.split("[\t ]");
        	if ((Integer.parseInt(tokens[0]) == 1) && (Integer.parseInt(tokens[1]) == 4)) {
        		Assert.assertEquals(false, Boolean.parseBoolean(s.split("[\t ]")[3]));
        	}
        	else if ((Integer.parseInt(tokens[0]) == 4) && (Integer.parseInt(tokens[1]) == 1)) {
        		Assert.assertEquals(false, Boolean.parseBoolean(s.split("[\t ]")[3]));
        	}
        	else {
            	Assert.assertEquals(true, Boolean.parseBoolean(s.split("[\t ]")[3]));        		
        	}
        	System.out.println(s);
        }
        System.out.println();
    }
    
}