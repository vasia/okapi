package ml.grafos.okapi.semimetric;

import java.util.Iterator;

import org.apache.giraph.aggregators.BasicAggregator;

public class UnlabeledEdgesAggregator extends BasicAggregator<UnlabeledEdgeHashSetWritable> {

	@Override
	public void aggregate(UnlabeledEdgeHashSetWritable value) {
		Iterator<UnlabeledEdge> iterator = value.iterator();
		if (iterator.hasNext()) {
			UnlabeledEdge edgeToAdd = iterator.next();
			getAggregatedValue().add(edgeToAdd);
		}
	}

	@Override
	public UnlabeledEdgeHashSetWritable createInitialValue() {
		return new UnlabeledEdgeHashSetWritable();
	}
}
