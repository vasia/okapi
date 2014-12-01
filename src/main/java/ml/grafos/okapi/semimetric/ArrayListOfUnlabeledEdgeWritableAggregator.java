package ml.grafos.okapi.semimetric;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;

@SuppressWarnings("rawtypes")
public class ArrayListOfUnlabeledEdgeWritableAggregator extends 
	DefaultImmutableClassesGiraphConfigurable 
	implements Aggregator<ArrayListOfUnlabeledEdgeWritable> {
	
	private ArrayListOfUnlabeledEdgeWritable value;
	
	public ArrayListOfUnlabeledEdgeWritableAggregator() {
		setAggregatedValue(createInitialValue());
	}
	
	@Override
	public void aggregate(ArrayListOfUnlabeledEdgeWritable other) {
		for (UnlabeledEdge e : other) {
			this.value.add(e);
		}
	}

	@Override
	public ArrayListOfUnlabeledEdgeWritable createInitialValue() {
		return new ArrayListOfUnlabeledEdgeWritable();
	}

	@Override
	public ArrayListOfUnlabeledEdgeWritable getAggregatedValue() {
		return value;
	}

	@Override
	public void setAggregatedValue(ArrayListOfUnlabeledEdgeWritable value) {
		this.value = value;		
	}

	@Override
	public void reset() {
		value = createInitialValue();		
	}
}
