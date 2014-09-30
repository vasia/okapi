/**
 * Copyright 2014 Grafos.ml
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ml.grafos.okapi.graphs;

import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.List;

import ml.grafos.okapi.io.formats.LongDoubleTextEdgeInputFormat;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.HashMapEdges;
import org.apache.giraph.io.formats.AdjacencyListTextVertexOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

public class AdamicAdarSimilarityTest {
	
	final double delta = 0.0001;

  @Test
  public void testExact() {
    String[] graph = { 
        "1 2 0.0",
        "2 1 0.0",
        "1 3 0.0",
        "3 1 0.0",
        "1 4 0.0",
        "4 1 0.0",
        "2 4 0.0",
        "4 2 0.0",
        "2 5 0.0",
        "5 2 0.0",
        "3 4 0.0",
        "4 3 0.0",
        "4 5 0.0",
        "5 4 0.0"
    };

    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(AdamicAdarSimilarity.SendFriendsListAndValue.class);
    conf.setMasterComputeClass(AdamicAdarSimilarity.MasterCompute.class);
    conf.setEdgeInputFormatClass(LongDoubleTextEdgeInputFormat.class);
    conf.setVertexOutputFormatClass(AdjacencyListTextVertexOutputFormat.class);
    conf.setOutEdgesClass(HashMapEdges.class);
   	conf.set("distance.conversion.enabled", "false");
    
    Iterable<String> results;
    try {
      results = InternalVertexRunner.run(conf, null, graph);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception occurred");
      return;
    }
    List<String> res = new LinkedList<String>();
    for (String string : results) {
      res.add(string);
      System.out.println(string);
      
      String[] output = string.split("[\t ]");
      if (Integer.parseInt(output[0]) == 1) {
    	  assertEquals(-1.098612, Double.parseDouble(output[1]), delta);
    	  assertEquals(2, Integer.parseInt(output[2]));
    	  assertEquals(-1.386294, Double.parseDouble(output[3]), delta);
    	  assertEquals(-1.386294, Double.parseDouble(output[5]), delta);
    	  assertEquals(-1.791759, Double.parseDouble(output[7]), delta);
      }
      if (Integer.parseInt(output[0]) == 3) {
    	  assertEquals(-0.693147, Double.parseDouble(output[1]), delta);
    	  assertEquals(-1.386294, Double.parseDouble(output[3]), delta);
    	  assertEquals(-1.098612, Double.parseDouble(output[5]), delta);
      }
      if (Integer.parseInt(output[0]) == 4) {
    	  assertEquals(-1.386294, Double.parseDouble(output[1]), delta);
    	  assertEquals(-1.791759, Double.parseDouble(output[3]), delta);
    	  assertEquals(-1.791759, Double.parseDouble(output[5]), delta);
    	  assertEquals(-1.098612, Double.parseDouble(output[7]), delta);
    	  assertEquals(-1.098612, Double.parseDouble(output[9]), delta);
      }

    }
  }

}