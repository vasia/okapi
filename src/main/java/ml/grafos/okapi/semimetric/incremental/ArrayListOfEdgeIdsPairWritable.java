package ml.grafos.okapi.semimetric.incremental;

import org.apache.giraph.utils.ArrayListWritable;

@SuppressWarnings("serial")
public class ArrayListOfEdgeIdsPairWritable extends ArrayListWritable<EdgeIdsPair> {

	public ArrayListOfEdgeIdsPairWritable() {
	}

	@Override
	public void setClass() {
		setClass(EdgeIdsPair.class);
	}


	@Override
	public boolean add(EdgeIdsPair e) {
		for (EdgeIdsPair pair : this) {
			if (pair.equals(e)) {
				return false;
			}
		}
		return super.add(e);
	}
}
