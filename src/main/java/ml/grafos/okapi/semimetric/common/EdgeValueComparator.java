package ml.grafos.okapi.semimetric.common;

import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;

@SuppressWarnings("rawtypes")
public class EdgeValueComparator<K extends Comparable,V extends Comparable> 
	implements Comparator<Map.Entry<K,V>> {

	@SuppressWarnings("unchecked")
	@Override
	public int compare(Entry<K, V> e1, Entry<K, V> e2) {
		 int res = e1.getValue().compareTo(e2.getValue());
		 if (res == 0) { 
			// equal values, compare vertex ids
			return e1.getKey().compareTo(e2.getKey()); 
		 }
		 else {
			 return res;
		 }
	}

	
}
