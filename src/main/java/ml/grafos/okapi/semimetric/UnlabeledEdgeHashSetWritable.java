package ml.grafos.okapi.semimetric;

import ml.grafos.okapi.common.data.HashSetWritable;

public class UnlabeledEdgeHashSetWritable extends HashSetWritable<UnlabeledEdge>{

	private static final long serialVersionUID = 1L;

	@Override
	public void setClass() {
		setClass(UnlabeledEdge.class);
	}

}
