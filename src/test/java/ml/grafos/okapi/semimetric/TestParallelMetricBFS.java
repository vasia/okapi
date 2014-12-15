package ml.grafos.okapi.semimetric;

import junit.framework.Assert;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.HashMapEdges;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

import ml.grafos.okapi.semimetric.io.EdgesWithValuesVertexOutputFormat;
import ml.grafos.okapi.semimetric.io.LongDoubleIntEdgeValueInputFormat;

public class TestParallelMetricBFS {
	
	/**
	 * Test with one unlabeled semi-metric edge. 
	 */
	@Test
	public void testOneUnlabeledSemimetric() throws Exception {
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
        conf.setMasterComputeClass(ParallelMetricBFS.MasterCompute.class);
        conf.setComputationClass(ParallelMetricBFS.PropagateUnlabeledEdges.class);
        conf.setEdgeInputFormatClass(LongDoubleIntEdgeValueInputFormat.class);
        conf.setVertexOutputFormatClass(EdgesWithValuesVertexOutputFormat.class);
        conf.setOutEdgesClass(HashMapEdges.class);

        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, null, graph);
        System.out.println("Testing one unlabeled semi-metric...");
        for (String s: results) {
        	// make sure that edges (1,2) and (2,1) have been marked as semi-metric
        	String[] output = s.split("[\t ]");
        	if ((Long.parseLong(output[0]) == 2) && (Long.parseLong(output[1]) == 1)) {
        		// edge (2, 1)
        		Assert.assertEquals(2, Integer.parseInt(output[3]));
        	}
        	if ((Long.parseLong(output[0]) == 1) && (Long.parseLong(output[1]) == 2)) {
        		// edge (1, 2)
        		Assert.assertEquals(2, Integer.parseInt(output[3]));
        	}
        	System.out.println(s);
        }
        System.out.println();
    }    
	
	/**
	 * Test with no unlabeled edges 
	 */
	@Test
	public void testNoUnlabeled() throws Exception {
        String[] graph = new String[] {
        		"1	2	9.0	true", "2	1	9.0	true",
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
        conf.setMasterComputeClass(ParallelMetricBFS.MasterCompute.class);
        conf.setComputationClass(ParallelMetricBFS.PropagateUnlabeledEdges.class);
        conf.setEdgeInputFormatClass(LongDoubleIntEdgeValueInputFormat.class);
        conf.setVertexOutputFormatClass(EdgesWithValuesVertexOutputFormat.class);
        conf.setOutEdgesClass(HashMapEdges.class);

        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, null, graph);
        System.out.println("Testing without any unlabeled edges...");
        int edgeCount = 0;
        for (String s: results) {
        	edgeCount++;
        	System.out.println(s);
        }
        System.out.println();
        Assert.assertEquals(24, edgeCount);
    }    
	
	/**
	 * Test with one metric unlabeled edge
	 */
	@Test
	public void testOneMetricUnlabeled() throws Exception {
        String[] graph = new String[] {
        		"1	2	9.0	false", "2	1	9.0	false",
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
        conf.setMasterComputeClass(ParallelMetricBFS.MasterCompute.class);
        conf.setComputationClass(ParallelMetricBFS.PropagateUnlabeledEdges.class);
        conf.setEdgeInputFormatClass(LongDoubleIntEdgeValueInputFormat.class);
        conf.setVertexOutputFormatClass(EdgesWithValuesVertexOutputFormat.class);
        conf.setOutEdgesClass(HashMapEdges.class);

        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, null, graph);
        System.out.println("Testing with one metric unlabeled...");
        for (String s: results) {
        	String[] output = s.split("[\t ]");
        	// make sure that no edges were marked as semi-metric
        	Assert.assertEquals(false, (Integer.parseInt(output[3]) == 2));
        	System.out.println(s);
        }
        System.out.println();
    }  
	
	/**
	 * Test with one metric unlabeled edge
	 * and one semi-metric unlabeled edge
	 */
	@Test
	public void testOneMetricOneSemimetricUnlabeled() throws Exception {
        String[] graph = new String[] {
        		"1	2	10.0	false", "2	1	10.0	false", // semi-metric unlabeled
        		"1	3	2.0	true",  "3	1	2.0	true",
        		"1	6	5.0	true",  "6	1	5.0	true",
        		"1	8	6.0	false",  "8	1	6.0	false",	// metric unlabeled
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
        conf.setMasterComputeClass(ParallelMetricBFS.MasterCompute.class);
        conf.setComputationClass(ParallelMetricBFS.PropagateUnlabeledEdges.class);
        conf.setEdgeInputFormatClass(LongDoubleIntEdgeValueInputFormat.class);
        conf.setVertexOutputFormatClass(EdgesWithValuesVertexOutputFormat.class);
        conf.setOutEdgesClass(HashMapEdges.class);

        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, null, graph);
        System.out.println("Testing with one metric unlabeled and one semi-metric unlabeled...");
        for (String s: results) {
        	// make sure that edges (1,2) and (2,1) have been marked semi-metric
        	String[] output = s.split("[\t ]");
        	if ((Long.parseLong(output[0]) == 2) && (Long.parseLong(output[1]) == 1)) {
        		// edge (2, 1)
        		Assert.assertEquals(2, Integer.parseInt(output[3]));
        	}
        	if ((Long.parseLong(output[0]) == 1) && (Long.parseLong(output[1]) == 2)) {
        		// edge (1, 2)
        		Assert.assertEquals(2, Integer.parseInt(output[3]));
        	}
        	// make sure edges (1, 8) and (8, 1) have not been marked as semi-metric
        	if ((Long.parseLong(output[0]) == 1) && (Long.parseLong(output[1]) == 8)) {
        		Assert.assertEquals(false, Integer.parseInt(output[3]) == 2);
        	}
        	if ((Long.parseLong(output[0]) == 8) && (Long.parseLong(output[1]) == 1)) {
        		Assert.assertEquals(false, Integer.parseInt(output[3]) == 2);
        	}
        	System.out.println(s);
        }
        System.out.println();
    }  
	
	/**
	 * Test with two metric unlabeled edges
	 * and two semi-metric unlabeled edges
	 */
	@Test
	public void testTwoMetricTwoSemimetricUnlabeled() throws Exception {
        String[] graph = new String[] {
        		"1	2	10.0	false", "2	1	10.0	false", // semi-metric unlabeled
        		"1	3	2.0	true",  "3	1	2.0	true",
        		"1	6	5.0	true",  "6	1	5.0	true",
        		"1	8	6.0	false",  "8	1	6.0	false",	// metric unlabeled
        		"2	5	2.0	true",  "5	2	2.0	true",
        		"3	4	3.0	false",  "4	3	3.0	false", // metric unlabeled
        		"4	5	2.0	true",  "5	4	2.0	true",
        		"4	7	4.0	true",  "7	4	4.0	true",
        		"6	7	8.0	true",  "7	6	8.0	true",
        		"8	9	2.0	true",  "9	8	2.0	true",
        		"9	10	1.0	true",  "10	9	1.0	true",
        		"9	11	1.0	true",  "11	9	1.0	true",
        		"5	6	20.0	false",  "6	5	20.0	false" // semi-metric unlabeled
                 };
	      	
        // run to check results correctness
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setMasterComputeClass(ParallelMetricBFS.MasterCompute.class);
        conf.setComputationClass(ParallelMetricBFS.PropagateUnlabeledEdges.class);
        conf.setEdgeInputFormatClass(LongDoubleIntEdgeValueInputFormat.class);
        conf.setVertexOutputFormatClass(EdgesWithValuesVertexOutputFormat.class);
        conf.setOutEdgesClass(HashMapEdges.class);

        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, null, graph);
        System.out.println("Testing with two metric unlabeled and two semi-metric unlabeled...");
        for (String s: results) {
        	// make sure that edges (1,2) and (2,1) have been removed
        	String[] output = s.split("[\t ]");
        	if ((Long.parseLong(output[0]) == 2) && (Long.parseLong(output[1]) == 1)) {
        		// edge (2, 1)
        		Assert.assertEquals(2, Integer.parseInt(output[3]));
        	}
        	if ((Long.parseLong(output[0]) == 1) && (Long.parseLong(output[1]) == 2)) {
        		// edge (1, 2)
        		Assert.assertEquals(2, Integer.parseInt(output[3]));
        	}
        	if ((Long.parseLong(output[0]) == 5) && (Long.parseLong(output[1]) == 6)) {
        		Assert.assertEquals(2, Integer.parseInt(output[3]));
        	}
        	if ((Long.parseLong(output[0]) == 6) && (Long.parseLong(output[1]) == 5)) {
        		Assert.assertEquals(2, Integer.parseInt(output[3]));
        	}

        	// make sure edges (1, 8) and (8, 1) have not been marked as semi-metric
        	if ((Long.parseLong(output[0]) == 1) && (Long.parseLong(output[1]) == 8)) {
        		Assert.assertEquals(false, Integer.parseInt(output[3]) == 2);
        	}
        	if ((Long.parseLong(output[0]) == 8) && (Long.parseLong(output[1]) == 1)) {
        		Assert.assertEquals(false, Integer.parseInt(output[3]) == 2);
        	}
        	if ((Long.parseLong(output[0]) == 3) && (Long.parseLong(output[1]) == 4)) {
        		Assert.assertEquals(false, Integer.parseInt(output[3]) == 2);
        	}
        	if ((Long.parseLong(output[0]) == 4) && (Long.parseLong(output[1]) == 3)) {
        		Assert.assertEquals(false, Integer.parseInt(output[3]) == 2);
        	}
        	System.out.println(s);
        }
        System.out.println();
    }  
}