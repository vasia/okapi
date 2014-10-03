package ml.grafos.okapi.incremental;

import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.List;

import ml.grafos.okapi.incremental.util.FloatBooleanPairEdgeInputFormat;
import ml.grafos.okapi.incremental.util.LongDistanceAndDegreeValueInputFormat;

import org.apache.giraph.io.formats.AdjacencyListTextVertexOutputFormat;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.HashMapEdges;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

public class TestIncrementalSSSP {
	
	/**
	 * Tests TODO:
	 * 1. delete an edge that does not belong to the SP-G: OK
	 * 2. delete an SP-edge that is close to the source: OK
	 * 3. delete an SP-edge which has a target vertex that has no out-SP-edges: OK 
	 * 4. pass a wrong event type number as parameter
	 * 5. give a non-existent edge as parameter
	 */

	String[] edges = new String[] {
    		"1 5 4 true",
    		"2 1 5 false",
    		"2 3 4 true",
    		"3 4 2 true",
    		"3 5 3 false",
    		"4 2 3 false",
    		"5 2 3 true",
    		"5 4 15 false"
    };
	
	String[] vertices = new String[] {
			"1 0 0",
			"2 7 1",
			"3 11 1",
			"4 13 1",
			"5 4 1"
	};
	
	@Test
	public void removeNonSPEdge() throws Exception {
		/**
		 * 1. Delete an edge that does not belong to the SP-G
		 */
		
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setInt("event.type", 2); // edge removal
        conf.setLong("event.edge.src", 2);
        conf.setLong("event.edge.trg", 1);
        
        conf.setComputationClass(IncrementalSSSP.CheckIfEdgeInSPG.class);
        conf.setMasterComputeClass(IncrementalSSSP.MasterCompute.class);
        conf.setEdgeInputFormatClass(FloatBooleanPairEdgeInputFormat.class);
        conf.setVertexInputFormatClass(LongDistanceAndDegreeValueInputFormat.class);
        conf.setVertexOutputFormatClass(AdjacencyListTextVertexOutputFormat.class);
        conf.setOutEdgesClass(HashMapEdges.class);
        Iterable<String> results = InternalVertexRunner.run(conf, vertices, edges);
        
        List<String> res = new LinkedList<String>();
        for (String s: results) {
        	res.add(s);
        	System.out.println(s);
        }
        assertEquals(5, res.size());
        //TODO: add check that all edge values remain unchanged
	}
	
	@Test
	public void removeSPEdgeCloseToSrc() throws Exception {
		/**
		 * 2. Delete an SP-edge that is close to the source
		 */
		
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setInt("event.type", 2); // edge removal
        conf.setLong("event.edge.src", 5);
        conf.setLong("event.edge.trg", 2);
        
        conf.setComputationClass(IncrementalSSSP.CheckIfEdgeInSPG.class);
        conf.setMasterComputeClass(IncrementalSSSP.MasterCompute.class);
        conf.setEdgeInputFormatClass(FloatBooleanPairEdgeInputFormat.class);
        conf.setVertexInputFormatClass(LongDistanceAndDegreeValueInputFormat.class);
        conf.setVertexOutputFormatClass(AdjacencyListTextVertexOutputFormat.class);
        conf.setOutEdgesClass(HashMapEdges.class);
        Iterable<String> results = InternalVertexRunner.run(conf, vertices, edges);
        
        List<String> res = new LinkedList<String>();
        for (String s: results) {
        	res.add(s);
        	System.out.println(s);
        }
        assertEquals(5, res.size());
        //TODO: add check that vertices 2, 3 and 4 reset their values to infinity
	}
	
	@Test
	public void removeSPEdgeFarFromSrc() throws Exception {
		/**
		 * 3. Delete an SP-edge which has a target vertex that has no out-SP-edges
		 */
		
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setInt("event.type", 2); // edge removal
        conf.setLong("event.edge.src", 3);
        conf.setLong("event.edge.trg", 4);
        
        conf.setComputationClass(IncrementalSSSP.CheckIfEdgeInSPG.class);
        conf.setMasterComputeClass(IncrementalSSSP.MasterCompute.class);
        conf.setEdgeInputFormatClass(FloatBooleanPairEdgeInputFormat.class);
        conf.setVertexInputFormatClass(LongDistanceAndDegreeValueInputFormat.class);
        conf.setVertexOutputFormatClass(AdjacencyListTextVertexOutputFormat.class);
        conf.setOutEdgesClass(HashMapEdges.class);
        Iterable<String> results = InternalVertexRunner.run(conf, vertices, edges);
        
        List<String> res = new LinkedList<String>();
        for (String s: results) {
        	res.add(s);
        	System.out.println(s);
        }
        assertEquals(5, res.size());
        //TODO: add check that vertex 4 resets its values to infinity
	}
	

}
