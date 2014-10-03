package ml.grafos.okapi.incremental;

import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.List;

import ml.grafos.okapi.incremental.util.FloatBooleanPairEdgeInputFormat;
import ml.grafos.okapi.incremental.util.LongDoubleInitVertexValueInputFormat;

import org.apache.giraph.io.formats.AdjacencyListTextVertexOutputFormat;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.HashMapEdges;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

public class TestAnnotateSPEdges {

	@Test
	public void test1() throws Exception {
		
		String[] edges = new String[] {
        		"1 5 4",
        		"2 1 5",
        		"2 3 4",
        		"3 4 2",
        		"3 5 3",
        		"4 2 3",
        		"5 2 3"
        };
		
		String[] vertices = new String[] {
				"1 0",
				"2 7",
				"3 11",
				"4 13",
				"5 4"
		};
		
		// run to check results correctness
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setComputationClass(AnnotateShortestPathsEdges.SendIdToNeighbors.class);
        conf.setMasterComputeClass(AnnotateShortestPathsEdges.MasterCompute.class);
        conf.setEdgeInputFormatClass(FloatBooleanPairEdgeInputFormat.class);
        conf.setVertexInputFormatClass(LongDoubleInitVertexValueInputFormat.class);
        conf.setVertexOutputFormatClass(AdjacencyListTextVertexOutputFormat.class);
        conf.setOutEdgesClass(HashMapEdges.class);
        Iterable<String> results = InternalVertexRunner.run(conf, vertices, edges);
        
        List<String> res = new LinkedList<String>();
        for (String s: results) {
        	res.add(s);
        	System.out.println(s);
        }
        assertEquals(5, res.size());
//        for (String s: results) {
//        	String[] tokens = s.split("[\t ]");
//        	if ((tokens[0] == "5") && (tokens[2] == "2")) {
//        		tokens[5] = "SPEdge";
//        	}
//        	if ((tokens[0] == "1") && (tokens[2] == "5")) {
//        		tokens[5] = "SPEdge";
//        	}
//        }
	}

}
