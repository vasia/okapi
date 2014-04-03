package ml.grafos.okapi.kmeans;

import ml.grafos.okapi.common.data.DoubleArrayListWritable;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.hadoop.io.DoubleWritable;

public class DoubleArrayListWritableAggregator extends DefaultImmutableClassesGiraphConfigurable 
	implements Aggregator<DoubleArrayListWritable> {
	
	private int dimensions; 
	private DoubleArrayListWritable value;

	public DoubleArrayListWritableAggregator() {
		value = createInitialValue();
	}
	
	@Override
	public void aggregate(DoubleArrayListWritable other) {
		for ( int i = 0; i < dimensions; i++ ) {
			value.set(i, new DoubleWritable(value.get(i).get() + other.get(i).get()));
		}
		
	}

	@Override
	public DoubleArrayListWritable createInitialValue() {
		dimensions = getConf().getInt("kmeans.points.dimensions", 0);
		if ( dimensions > 0 ) {
			DoubleArrayListWritable toReturn = new DoubleArrayListWritable();
			for ( int i=0; i < dimensions; i++ ) {
				toReturn.add(new DoubleWritable(0));
			}
			return toReturn;
		}
		else 
			throw new IllegalArgumentException
			("The size and dimensions of the cluster centers should be greater than zero");
	}
	

	@Override
	public DoubleArrayListWritable getAggregatedValue() {
		return value;
	}

	@Override
	public void setAggregatedValue(DoubleArrayListWritable value) {
		this.value = value;
		
	}

	@Override
	public void reset() {
		value = createInitialValue();
		
	}



}
