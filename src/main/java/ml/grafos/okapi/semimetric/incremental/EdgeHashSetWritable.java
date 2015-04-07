package ml.grafos.okapi.semimetric.incremental;

import ml.grafos.okapi.common.data.HashSetWritable;

public class EdgeHashSetWritable extends HashSetWritable<EdgeIdsPair>{

	private static final long serialVersionUID = 1L;

	@Override
	public void setClass() {
		setClass(EdgeIdsPair.class);
	}

}
