package ml.grafos.okapi.semimetric.incremental;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;

@SuppressWarnings("rawtypes")
public class ArrayListOfEdgeIdsPairWritableAggregator extends 
	DefaultImmutableClassesGiraphConfigurable 
	implements Aggregator<ArrayListOfEdgeIdsPairWritable> {
	
	private ArrayListOfEdgeIdsPairWritable value;
	
	public ArrayListOfEdgeIdsPairWritableAggregator() {
		setAggregatedValue(createInitialValue());
	}
	
	@Override
	public void aggregate(ArrayListOfEdgeIdsPairWritable other) {
		for (EdgeIdsPair e : other) {
			this.value.add(e);
		}
	}

	@Override
	public ArrayListOfEdgeIdsPairWritable createInitialValue() {
		return new ArrayListOfEdgeIdsPairWritable();
	}

	@Override
	public ArrayListOfEdgeIdsPairWritable getAggregatedValue() {
		return value;
	}

	@Override
	public void setAggregatedValue(ArrayListOfEdgeIdsPairWritable value) {
		this.value = value;		
	}

	@Override
	public void reset() {
		value = createInitialValue();		
	}
}
