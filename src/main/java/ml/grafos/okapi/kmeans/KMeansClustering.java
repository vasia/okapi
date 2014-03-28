package ml.grafos.okapi.kmeans;

import java.io.IOException;

import org.apache.giraph.aggregators.matrix.dense.DoubleDenseVector;
import org.apache.giraph.aggregators.matrix.dense.LongDenseVector;
import org.apache.giraph.aggregators.matrix.dense.LongDenseVectorSumAggregator;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.python.modules.math;

/**
 * 
 * The k-means clustering algorithm partitions <code>N</code> data points (observations) into <code>k</code> clusters.
 * The input consists of data points with a <code>pointID</code> and a vector of coordinates.
 * 
 * The algorithm is iterative and works as follows.
 * In the initialization phase, <code>k</code> clusters are chosen from the input points at random.
 * 
 * In each iteration:
 * 1. each data point is assigned to the cluster center which is closest to it, by means of euclidean distance
 * 2. new cluster centers are recomputed, by calculating the arithmetic mean of the assigned points
 * 
 * Convergence is reached when the positions of the cluster centers do not change.
 *
 * http://en.wikipedia.org/wiki/K-means_clustering
 * 
 */
public class KMeansClustering {

  /**
   * Used to store the cluster centers coordinates.
   * It aggregates the vector coordinates of points assigned to cluster centers,
   * in order to compute the means as the coordinates of the new cluster centers.
   */
  private static String CLUSTER_CENTERS_VECTORS = "cluster.centers.vectors";

  /**
   * Aggregator used to store the number of points assigned to each cluster center
   */
  private static String ASSIGNED_POINTS_COUNT = "assigned.points.count";
  
  /** Maximum number of iterations */
  public static final String MAX_ITERATIONS = "kmeans.iterations";
  /** Default value for iterations */
  public static final int ITERATIONS_DEFAULT = 10;
  /** Number of cluster centers */
  public static final String CLUSTER_CENTERS_COUNT = "kmeans.cluster.centers.count";
  /** Default number of cluster centers */
  public static final int CLUSTER_CENTERS_COUNT_DEFAULT = 3;
  /** Dimensions of the input points*/

  public static class KMeansClusteringComputation extends BasicComputation<
  LongWritable, DoubleDenseVector, NullWritable, NullWritable> {

	@Override
	public void compute(
			Vertex<LongWritable, DoubleDenseVector, NullWritable> vertex,
			Iterable<NullWritable> messages) throws IOException {
		// read the cluster centers coordinates
		VectorOfDoubleDenseVectors clusterCenters = getAggregatedValue(CLUSTER_CENTERS_VECTORS);
		// find the closest center
		int centerId = findClosestCenter(clusterCenters, vertex.getValue());
		// aggregate this point's coordinates to the cluster centers aggregator
		aggregate(CLUSTER_CENTERS_VECTORS, new VectorOfDoubleDenseVectors(
				clusterCenters.getSize(), clusterCenters.getVectorDimensions(),
				centerId, vertex.getValue()));
		// increase the count of assigned points for this cluster center
		LongDenseVector vectorToAdd = new LongDenseVector();
		vectorToAdd.set(centerId, 1);
		aggregate(ASSIGNED_POINTS_COUNT, vectorToAdd);
	}

	/**
	 * finds the closest center to the given point
	 * by minimizing the Euclidean distance
	 * 
	 * @param clusterCenters
	 * @param value
	 * @return the index of the cluster center in the clusterCenters vector
	 */
	private int findClosestCenter(VectorOfDoubleDenseVectors clusterCenters,
			DoubleDenseVector value) {
		double minDistance = Double.MAX_VALUE;
		double distanceFromI;
		int clusterIndex = 0;
		for ( int i = 0; i < clusterCenters.getSize(); i++ ) {
			distanceFromI = euclideanDistance(clusterCenters.getVectorList().get(i), value, 
					clusterCenters.getVectorDimensions()); 
			if ( distanceFromI > minDistance ) {
				minDistance = distanceFromI;
				clusterIndex = i;
			}
		}
		return clusterIndex;
	}

	/**
	 * Calculates the Euclidean distance between two vectors of doubles
	 * 
	 * @param v1
	 * @param v2
	 * @param dim 
	 * @return
	 */
	private double euclideanDistance(DoubleDenseVector v1, DoubleDenseVector v2, int dim) {
		double distance = 0;		
		for ( int i = 0; i < dim; i++ ) {
			distance += math.sqrt(math.pow(v1.get(i) - v2.get(i), 2));
		}
		return distance;
	}
  }

  /**
   * The Master calculates the new cluster centers
   * and also checks for convergence
   */
  public static class MasterCompute extends DefaultMasterCompute {
	  private int maxIterations;
	  private VectorOfDoubleDenseVectors currentClusterCenters;
	  private int clustersCount;
	    
    @Override
    public final void initialize() throws InstantiationException,
        IllegalAccessException {
    	maxIterations = getContext().getConfiguration().getInt(MAX_ITERATIONS, 
    			ITERATIONS_DEFAULT);
    	clustersCount = getContext().getConfiguration().getInt(CLUSTER_CENTERS_COUNT, 
    			CLUSTER_CENTERS_COUNT_DEFAULT);
      registerAggregator(CLUSTER_CENTERS_VECTORS, VectorOfDoubleDenseVectorsSumAggregator.class);
      registerAggregator(ASSIGNED_POINTS_COUNT, LongDenseVectorSumAggregator.class); //TODO: extend and customize this aggregator class
    }

    @Override
    public final void compute() {
      long superstep = getSuperstep();
      if (superstep == 0) {
    	  // initialize the cluster centers as random points from the input
    	  // and store the value for convergence comparison
    	  currentClusterCenters = selectRandomCenters();
    	  setAggregatedValue(CLUSTER_CENTERS_VECTORS, currentClusterCenters);
      }
      else {
	      VectorOfDoubleDenseVectors clusterCentersSum = getAggregatedValue(CLUSTER_CENTERS_VECTORS);
	      LongDenseVector pointsCounts = getAggregatedValue(ASSIGNED_POINTS_COUNT);
	      // compute the new centers positions
	      VectorOfDoubleDenseVectors newClusters = computeClusterCenters(clusterCentersSum, 
	    		  pointsCounts);
	      // check for convergence
	      if ( (superstep > maxIterations) || (clusterPositionsDiff(currentClusterCenters, newClusters)) ) {
	    	  haltComputation();
	      }
	      else {
	    	  // update the aggregator with the new cluster centers
	    	  setAggregatedValue(CLUSTER_CENTERS_VECTORS, newClusters);
	    	  currentClusterCenters = newClusters;
	      } 
      }
    }

	private boolean clusterPositionsDiff(
			VectorOfDoubleDenseVectors currentClusterCenters,
			VectorOfDoubleDenseVectors newClusters) {
		final double E = 0.0001f;
		return false;
	}

	private VectorOfDoubleDenseVectors selectRandomCenters() {
		return null;
	}

	private VectorOfDoubleDenseVectors computeClusterCenters(
			VectorOfDoubleDenseVectors clusterCentersSum, LongDenseVector pointsCounts) {
		return null;
	}
  }
}
