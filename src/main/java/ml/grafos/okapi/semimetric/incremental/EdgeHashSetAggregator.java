package ml.grafos.okapi.semimetric.incremental;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;

@SuppressWarnings("rawtypes")
public class EdgeHashSetAggregator extends DefaultImmutableClassesGiraphConfigurable
	implements Aggregator<EdgeHashSetWritable> {

	private EdgeHashSetWritable value;

	public EdgeHashSetAggregator() {
		setAggregatedValue(createInitialValue());
	}
	
	@Override
	public void aggregate(EdgeHashSetWritable set) {
		for (EdgeIdsPair pair : this.value) {
			set.add(pair);
		}
		setAggregatedValue(set);
	}

	@Override
	public EdgeHashSetWritable createInitialValue() {
		return new EdgeHashSetWritable();
	}

	@Override
	public EdgeHashSetWritable getAggregatedValue() {
		return value;
	}

	@Override
	public void setAggregatedValue(EdgeHashSetWritable set) {
		this.value = set;
	}

	@Override
	public void reset() {
		value = createInitialValue();
	}
}
