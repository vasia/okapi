package ml.grafos.okapi.kmeans;

import ml.grafos.okapi.common.data.DoubleArrayListWritable;
import org.apache.giraph.aggregators.BasicAggregator;

public class DoubleArrayListWritableAggregator extends BasicAggregator<DoubleArrayListWritable> {

	@Override
	public void aggregate(DoubleArrayListWritable other) {
		DoubleArrayListWritable value = getAggregatedValue();
		if ( value.size() == 0 ) {
			// first-time creation
			for ( int i = 0; i < other.size(); i ++ ) {
				value.add(other.get(i));
			}
			setAggregatedValue(value);
		}
		else if ( getAggregatedValue().size() < other.size() ) {
			throw new IndexOutOfBoundsException("The value to be aggregated " +
					"cannot have larger size than the aggregator value");
		}
		else {
			for ( int i = 0; i < other.size(); i ++ ) {
				value.get(i).set(value.get(i).get() + other.get(i).get());
			}
		}
		
	}

	@Override
	public DoubleArrayListWritable createInitialValue() {
		return new DoubleArrayListWritable();
	}
	
}
