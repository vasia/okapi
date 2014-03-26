package ml.grafos.okapi.kmeans;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.giraph.aggregators.matrix.dense.DoubleDenseVector;

public class VectorOfDoubleDenseVectorsSumAggregator extends BasicAggregator<VectorOfDoubleDenseVectors> {
	
	@Override
	public void aggregate(VectorOfDoubleDenseVectors value) {
		// do nothing		
	}

	@Override
	public VectorOfDoubleDenseVectors createInitialValue() {
		return new VectorOfDoubleDenseVectors();
	}
	
	public VectorOfDoubleDenseVectors createInitialValue(int size, int dimensions) {
		return new VectorOfDoubleDenseVectors(size, dimensions);
	}
	
	/**
	 * element-by-element addition of the vector v 
	 * to the vector in position index
	 * 
	 * @param v
	 * @param index
	 */
	public void aggregate(DoubleDenseVector v, int index) {
		VectorOfDoubleDenseVectors value = getAggregatedValue();
		if ( index > value.getSize() - 1 )
			throw new IndexOutOfBoundsException();
		getAggregatedValue().getVectorList().get(index).add(v);
	}

}
