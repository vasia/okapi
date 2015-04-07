package ml.grafos.okapi.semimetric;

import junit.framework.Assert;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.HashMapEdges;
import org.apache.giraph.io.formats.AdjacencyListTextVertexOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

import ml.grafos.okapi.semimetric.incremental.CalculateMutationCost;
import ml.grafos.okapi.semimetric.io.LongBooleanEdgeValueInputFormat;

public class TestCalculateAlternativePaths {
	
	/**
	 * Test removing a semi-metric edge. 
	 */
	@Test
	public void testSemimetricRemoval() throws Exception {
        String[] graph = new String[] {
        		"1	2	false", "2	1	false",
        		"1	3	false",  "3	1	false",
        		"1	6	true",  "6	1	true",
        		"1	8	false",  "8	1	false",
        		"2	5	false",  "5	2	false",
        		"3	4	false",  "4	3	false",
        		"4	5	false",  "5	4	false",
        		"4	7	false",  "7	4	false",
        		"6	7	false",  "7	6	false",
        		"8	9	false",  "9	8	false",
        		"9	10	false",  "10	9	false",
        		"9	11	false",  "11	9	false"
                 };
	      	
        // run to check results correctness
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setMasterComputeClass(CalculateMutationCost.MasterCompute.class);
        conf.setComputationClass(CalculateMutationCost.CheckSemiMetric.class);
        conf.setEdgeInputFormatClass(LongBooleanEdgeValueInputFormat.class);
        conf.setVertexOutputFormatClass(AdjacencyListTextVertexOutputFormat.class);
        conf.setOutEdgesClass(HashMapEdges.class);
        
        // set configuration input parameters
       	conf.set("event.type", "2");
       	conf.set("event.edge.src", "1");
       	conf.set("event.edge.trg", "6");

        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, null, graph);
        System.out.println("Testing removal of a semi-metric edge");
        for (String s: results) {
        	// print results
        	System.out.println(s);
        }
        System.out.println();
    }    
}