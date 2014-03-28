package ml.grafos.okapi.kmeans;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.aggregators.matrix.dense.DoubleDenseVector;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;

public class VectorOfDoubleDenseVectorsSumAggregator 
	extends DefaultImmutableClassesGiraphConfigurable 
	implements Aggregator<VectorOfDoubleDenseVectors> {
	
	private int size; // the number of the vector members
	private int dimensions; // the size of each vector member
	private VectorOfDoubleDenseVectors value;
	
	public VectorOfDoubleDenseVectorsSumAggregator() {
		value = createInitialValue();
	}
	
	@Override
	public void aggregate(VectorOfDoubleDenseVectors other) {
		for ( int i = 0; i < size; i ++ ) {
			if ( other.getVectorList().get(i) != null ) {
				value.getVectorList().get(i).add(other.getVectorList().get(i));
			}
		}
	}

	@Override
	public VectorOfDoubleDenseVectors createInitialValue() {
		size = getConf().getInt("aggregator.size", 0);
		dimensions = getConf().getInt("aggregator.dimensions", 0);
		if ( (size > 0) && (dimensions > 0)) {
			return new VectorOfDoubleDenseVectors(size, dimensions);
		}
		else 
			throw new IllegalArgumentException
			("The size and dimensions of a VectorOfDoubleDenseVectors should be greater than zero");
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

	@Override
	public VectorOfDoubleDenseVectors getAggregatedValue() {
		return value;
	}

	@Override
	public void setAggregatedValue(VectorOfDoubleDenseVectors value) {
		this.value = value;		
	}

	@Override
	public void reset() {
		value = createInitialValue();
	}

}
