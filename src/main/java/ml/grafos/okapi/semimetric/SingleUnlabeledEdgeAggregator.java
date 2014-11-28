package ml.grafos.okapi.semimetric;

import org.apache.giraph.aggregators.BasicAggregator;

public class SingleUnlabeledEdgeAggregator extends BasicAggregator<UnlabeledEdge> {

	// the value is set only by the master
	@Override
	public void aggregate(UnlabeledEdge value) {
		// do nothing
	}

	@Override
	public UnlabeledEdge createInitialValue() {
		return new UnlabeledEdge();
	}
}
