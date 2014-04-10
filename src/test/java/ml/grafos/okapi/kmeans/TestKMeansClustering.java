package ml.grafos.okapi.kmeans;

import static org.junit.Assert.*;

import java.util.Set;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;

public class TestKMeansClustering {

	@Test
	public void test() throws Exception {
        String[] graph = new String[] {
        		"1,1.0	1.0",
        		"2,1.5	2.0",
        		"3,3.0	4.0",
        		"4,5.0	7.0",
        		"5,3.5	5.0",
        		"6,4.5	5.0",
        		"7,3.5	4.5"
                 };
     
        // run to check results correctness
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setInt(ArrayListOfDoubleArrayListWritableAggregator.CLUSTER_CENTERS_COUNT, 2);
		conf.setInt(ArrayListOfDoubleArrayListWritableAggregator.POINTS_COUNT, 7);
        conf.setMasterComputeClass(KMeansClustering.KMeansMasterCompute.class);
        conf.setComputationClass(KMeansClustering.RandomCentersInitialization.class);
        conf.setVertexInputFormatClass(KMeansTextInputFormat.class);
        conf.setOutEdgesClass(NullOutEdges.class);
        conf.setVertexOutputFormatClass(KMeansTextOutputFormat.class);
        conf.setInt(KMeansClustering.CLUSTER_CENTERS_COUNT, 2);
        conf.setInt(KMeansClustering.DIMENSIONS, 2);
        conf.setInt(KMeansClustering.POINTS_COUNT, 7);


        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, graph);
        SetMultimap<Integer,Integer> clusters = parseResults(results);

        Set<Integer> clusterIDs = clusters.keySet();
        assertEquals(2, clusterIDs.size());
        
        Set<Integer> clusterOne = clusters.get(0);
        Set<Integer> clusterTwo = clusters.get(1);
        if ( (clusterOne.size() == 2) && (clusterTwo.size() == 5)) {
        	assertTrue(clusterOne.contains(1));
        	assertTrue(clusterOne.contains(2));
            assertEquals(5, clusterTwo.size());
            assertTrue(clusterTwo.contains(3));
            assertTrue(clusterTwo.contains(4));
            assertTrue(clusterTwo.contains(5));
            assertTrue(clusterTwo.contains(6));
            assertTrue(clusterTwo.contains(7));
        }
        else  if ( (clusterOne.size() == 5) && (clusterTwo.size() == 2)) {
        	assertTrue(clusterTwo.contains(1));
        	assertTrue(clusterTwo.contains(2));
            assertEquals(5, clusterOne.size());
            assertTrue(clusterOne.contains(3));
            assertTrue(clusterOne.contains(4));
            assertTrue(clusterOne.contains(5));
            assertTrue(clusterOne.contains(6));
            assertTrue(clusterOne.contains(7));
        }
        else {
        	fail("Wrong cluster sizes");
        }
    }

    private SetMultimap<Integer,Integer> parseResults(
            Iterable<String> results) {
        SetMultimap<Integer,Integer> clusters = HashMultimap.create();
        for (String result : results) {
            Iterable<String> parts = Splitter.on(',').split(result);
            int point = Integer.parseInt(Iterables.get(parts, 0));
            int cluster = Integer.parseInt(Iterables.get(parts, 1));
            clusters.put(cluster, point);
        }
        return clusters;
    }

}
