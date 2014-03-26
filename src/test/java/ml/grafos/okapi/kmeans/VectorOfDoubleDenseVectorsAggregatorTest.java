package ml.grafos.okapi.kmeans;

import static org.junit.Assert.*;

import java.util.Random;

import org.apache.giraph.aggregators.matrix.dense.DoubleDenseVector;
import org.apache.giraph.utils.WritableUtils;
import org.junit.Test;

public class VectorOfDoubleDenseVectorsAggregatorTest {
	private static double E = 0.0001f;

	@Test
	public void testVectorAdd() {
		// create a vector for 5 centers and 2-dim points
		VectorOfDoubleDenseVectors v = new VectorOfDoubleDenseVectors(5, 2);
		VectorOfDoubleDenseVectorsSumAggregator vectorAggr = new VectorOfDoubleDenseVectorsSumAggregator();
		vectorAggr.setAggregatedValue(v);
		
		assertEquals(5, v.getVectorList().size());
		assertEquals(5, v.getSize());
		
		// default values should be 0
		assertEquals(0.0, v.getVectorList().get(0).get(0), E);
		assertEquals(0.0, v.getVectorList().get(1).get(1), E);
		
		// create a vector [1.0, 2.0] for addition
		DoubleDenseVector other1 = new DoubleDenseVector(2);
		other1.set(0, 1.0);
		other1.set(1, 2.0);
		
		// create a vector [1.5, 2.2] for addition
		DoubleDenseVector other2 = new DoubleDenseVector(2);
		other2.set(0, 1.5);
		other2.set(1, 2.2);
		
		// add other1 to the first vector
		vectorAggr.aggregate(other1, 0);
		assertEquals(1.0, vectorAggr.getAggregatedValue().getVectorList().get(0).get(0), E);
		
		// add other2 to the first vector
		vectorAggr.aggregate(other2, 0);
		assertEquals(2.5, vectorAggr.getAggregatedValue().getVectorList().get(0).get(0), E);
		assertEquals(4.2, vectorAggr.getAggregatedValue().getVectorList().get(0).get(1), E);	
	}
	
	  @Test
	  public void testVectorSerialize() throws Exception {
	    int size = 100;
	    int dim = 10;

	    // Serialize from
	    VectorOfDoubleDenseVectors from = new VectorOfDoubleDenseVectors(size, dim);
	    
	    DoubleDenseVector v1 = new DoubleDenseVector(dim);
	    DoubleDenseVector v2 = new DoubleDenseVector(dim);
	    DoubleDenseVector v3 = new DoubleDenseVector(dim);
	    
	    // insert random double values in the vectors
	    populateWithRandom(v1, dim);
	    populateWithRandom(v2, dim);
	    populateWithRandom(v3, dim);
	    
	    from.getVectorList().set(0, v1);
	    from.getVectorList().set(30, v2);
	    from.getVectorList().set(73, v3);
	    
	    byte[] data = WritableUtils.writeToByteArray(from, from);

	    // De-serialize to
	    VectorOfDoubleDenseVectors to1 = new VectorOfDoubleDenseVectors(size, dim);
	    VectorOfDoubleDenseVectors to2 = new VectorOfDoubleDenseVectors(size, dim);
	    WritableUtils.readFieldsFromByteArray(data, to1, to2);
	    
	    // all the vectors should be equal
	    for (int i = 0; i < size; ++i) {
	    	for (int j = 0; j < dim; j++) {
	    		assertEquals(from.getVectorList().get(i).get(j), to1.getVectorList().get(i).get(j), E);
	    		assertEquals(from.getVectorList().get(i).get(j), to2.getVectorList().get(i).get(j), E);
	    	}
	    }
	  }

	private void populateWithRandom(DoubleDenseVector v, int size) {
		for (int i = 0; i < size; i++) {
			Random r = new Random();
			double randomValue = 100.0 * r.nextDouble();
			v.set(i, randomValue);
		}
		
	}

}
