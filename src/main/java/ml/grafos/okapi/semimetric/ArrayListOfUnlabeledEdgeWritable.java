package ml.grafos.okapi.semimetric;

import org.apache.giraph.utils.ArrayListWritable;

public class ArrayListOfUnlabeledEdgeWritable extends ArrayListWritable<UnlabeledEdge> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1547484036891955472L;

	@Override
	public void setClass() {
		setClass(UnlabeledEdge.class);
	}


}
