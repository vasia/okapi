//package ml.grafos.okapi.semimetric;
//
//import junit.framework.Assert;
//
//import org.apache.giraph.conf.GiraphConfiguration;
//import org.apache.giraph.utils.InternalVertexRunner;
//import org.junit.Test;
//
//import ml.grafos.okapi.io.formats.LongDoubleTextEdgeInputFormat;
//import ml.grafos.okapi.io.formats.VertexWithDoubleValueDoubleEdgeTextOutputFormat;
//import ml.grafos.okapi.semimetric.common.TreeSetOutEdges;
//
//public class TestMetricBFS {
//	
//	@Test
//	public void testMetricBFS() throws Exception {
//        String[] graph = new String[] {
//        		"1	2	10.0", "2	1	10.0",
//        		"1	3	2.0",  "3	1	2.0",
//        		"1	6	5.0",  "6	1	5.0",
//        		"1	3	2.0",  "3	1	2.0",
//        		"1	8	6.0",  "8	1	6.0",
//        		"1	3	2.0",  "3	1	2.0",
//        		"2	5	2.0",  "5	2	2.0",
//        		"3	4	3.0",  "4	3	3.0",
//        		"4	5	2.0",  "5	4	2.0",
//        		"4	7	4.0",  "7	4	4.0",
//        		"1	3	2.0",  "3	1	2.0",
//        		"6	7	8.0",  "7	6	8.0",
//        		"8	9	2.0",  "9	8	2.0",
//        		"9	10	1.0",  "10	9	1.0",
//        		"9	11	1.0",  "11	9	1.0"
//                 };
//	      	
//        // run to check results correctness
//        GiraphConfiguration conf = new GiraphConfiguration();
//        conf.setComputationClass(MetricBFS.class);
//        conf.setEdgeInputFormatClass(LongDoubleTextEdgeInputFormat.class);
//        conf.setVertexOutputFormatClass(VertexWithDoubleValueDoubleEdgeTextOutputFormat.class);
//        conf.setOutEdgesClass(TreeSetOutEdges.class);
//        conf.setLong("bfs.source.id", 1);
//        conf.setLong("bfs.target.id", 2);
//        conf.setFloat("bfs.edge.weight", 10f);
//
//        // run internally
//        Iterable<String> results = InternalVertexRunner.run(conf, null, graph);
//        System.out.println("Testing metric bfs...");
//        for (String s: results) {
//        	String[] output = s.split("[\t ]");
//        	if (Long.parseLong(output[0]) == 2) {
//        		Assert.assertEquals(9.0, Double.parseDouble(output[1]), 0.0001);
//        	}
//        	System.out.println(s);
//        }
//        System.out.println();
//    }    
//}