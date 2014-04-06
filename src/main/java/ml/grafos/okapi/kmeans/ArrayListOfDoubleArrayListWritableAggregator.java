package ml.grafos.okapi.kmeans;

import java.util.Random;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;

public class ArrayListOfDoubleArrayListWritableAggregator extends 
	DefaultImmutableClassesGiraphConfigurable 
	implements Aggregator<ArrayListOfDoubleArrayListWritable> {

	private int k; // the number of the cluster centers
	private int pointsCount; // the number of input points
	private ArrayListOfDoubleArrayListWritable value;
	
	/**
	 * Used to randomly select initial points for k-means
	 * If the size of the current list is less than k (#centers)
	 * then the element is appended in the list
	 * else it replaces an element in a random position
	 * with probability k/N, where N is the total number of points
	 * 
	 * @param other should contain a single element
	 */
	@Override
	public void aggregate(ArrayListOfDoubleArrayListWritable other) {
		if ( other.size() != 1 ) {
			throw new IllegalArgumentException
			("The value to be aggregated should contain a single element!");
		}
		if ( getAggregatedValue().size() < k ) {
			// append in the current list
			value.add(other.get(0)); 
		}
		else  {
			Random ran = new Random();
			int index = ran.nextInt(k-1);
			if (Math.random() > ((double) k / (double) pointsCount) ) {
				value.set(index, other.get(0));
			}
		}
		
	}

	@Override
	public ArrayListOfDoubleArrayListWritable createInitialValue() {
		k = getConf().getInt("kmeans.cluster.centers.count", 0);
		pointsCount = getConf().getInt("kmeans.points.count", 0);
		if ( (k > 0) && (pointsCount > 0) ) {
			return new ArrayListOfDoubleArrayListWritable();
		}
		else {
			throw new IllegalArgumentException
			("The number of cluster centers and the number of input points should be greater than 0!");
		}
	}

	@Override
	public ArrayListOfDoubleArrayListWritable getAggregatedValue() {
		return value;
	}

	@Override
	public void setAggregatedValue(ArrayListOfDoubleArrayListWritable value) {
		this.value = value;		
	}

	@Override
	public void reset() {
		value = createInitialValue();		
	}

	
	

}
