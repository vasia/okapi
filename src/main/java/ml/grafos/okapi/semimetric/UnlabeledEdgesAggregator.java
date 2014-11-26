package ml.grafos.okapi.semimetric;

import org.apache.giraph.aggregators.BasicAggregator;

public class UnlabeledEdgesAggregator extends BasicAggregator<UnlabeledEdgeHashSetWritable> {

	@Override
	public void aggregate(UnlabeledEdgeHashSetWritable value) {
		UnlabeledEdgeHashSetWritable set = getAggregatedValue();
		UnlabeledEdge edgeToAdd = value.iterator().next();
		set.add(edgeToAdd);
		setAggregatedValue(set);
	}

	@Override
	public UnlabeledEdgeHashSetWritable createInitialValue() {
		return new UnlabeledEdgeHashSetWritable();
	}
}
