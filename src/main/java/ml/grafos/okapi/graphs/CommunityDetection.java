package ml.grafos.okapi.graphs;

import java.io.IOException;
import java.util.HashMap;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/** 
 * Simple community detection implementation
 * based on the algorithm from http://arxiv.org/pdf/0808.2633.pdf
 *
 */
public class CommunityDetection extends 
	BasicComputation<LongWritable, LongWritable, DoubleWritable, LabelWithScoreWritable> {
	
	 /** Default number of supersteps */
	public static final int MAX_SUPERSTEPS_DEFAULT = 30;
	/** Property name for number of supersteps */
	public static final String MAX_SUPERSTEPS = "community.detection.max.supersteps";
	
	/** Default value for delta */
	public static final float DELTA_DEFAULT = 0.5f;
	/** Property name for delta */
	public static final String DELTA = "community.detection.delta";
	
	/** Default value for preference */
	public static final int PREFERENCE_DEFAULT = 1;
	/** Property name for delta */
	public static final String PREFERENCE = "community.detection.preference";

	private long label;
	private double score;
	private HashMap<Long, Double> receivedLabelsWithScores = new HashMap<Long, Double>();
	private HashMap<Long, Double> labelsWithHighestScore = new HashMap<Long, Double>();
	
	private int max_supersteps;
	private float delta;
	private int preference;
	
	
	@Override
	  public void preSuperstep() {
		max_supersteps = getContext().getConfiguration().getInt(
			      MAX_SUPERSTEPS, MAX_SUPERSTEPS_DEFAULT);
		delta = getContext().getConfiguration().getFloat(
			      DELTA, DELTA_DEFAULT);
		preference = getContext().getConfiguration().getInt(
			      PREFERENCE, PREFERENCE_DEFAULT);
	  }

	@Override
	public void compute(
			Vertex<LongWritable, LongWritable, DoubleWritable> vertex,
			Iterable<LabelWithScoreWritable> messages) throws IOException {
		
		// init labels and their scores
		if (getSuperstep() == 0) {
			score = 1.0;
			label = vertex.getId().get();
			// send out initial messages
			for (Edge<LongWritable, DoubleWritable> e: vertex.getEdges()){
				LongWritable neighbor = e.getTargetVertexId();
				sendMessage(neighbor, new LabelWithScoreWritable(label, 
						score * vertex.getEdgeValue(neighbor).get() * 
						Math.pow(vertex.getNumEdges(), preference)));
			}
		}
		// receive labels from neighbors
		// compute the new score for each label
		// choose the label with the highest score as the new label
		// and re-score the newly chosen label
		else if (getSuperstep() < max_supersteps) {
			label = vertex.getValue().get();
			
			for (LabelWithScoreWritable m: messages) {
				long receivedLabel = m.getLabel();
				double receivedScore = m.getScore();
				
				// the label has been received before
				if (receivedLabelsWithScores.containsKey(receivedLabel)) {
					double newScore = receivedScore + receivedLabelsWithScores.get(receivedLabel);
					receivedLabelsWithScores.put(receivedLabel, newScore);
				}
				// first time we receive this label
				else {
					receivedLabelsWithScores.put(receivedLabel, receivedScore);
				}
				
				// during the first superstep, each neighbor has a different label
				// thus, there is no need for the highest-score map structure
				if (getSuperstep() > 1) {
					// store label with the highest score
					if (labelsWithHighestScore.containsKey(receivedLabel)) {
						double currentScore = labelsWithHighestScore.get(receivedLabel);
						if (currentScore < receivedScore) {
							// record the highest score
							labelsWithHighestScore.put(receivedLabel, receivedScore);
						}
					}
					else {
						// first time we see this label
						labelsWithHighestScore.put(receivedLabel, receivedScore);
					}
				}
			}
			// find the label with the highest score from the ones received
			double maxScore = -Double.MAX_VALUE;
			long maxScoreLabel = label;
			for (long curLabel : receivedLabelsWithScores.keySet()) {
				if (receivedLabelsWithScores.get(curLabel) > maxScore) {
					maxScore = receivedLabelsWithScores.get(curLabel);
					maxScoreLabel = curLabel;
				}
			}
			// find the highest score of maxScoreLabel in labelsWithHighestScore
			// the edge data set always contains self-edges for every node
			
			// if it's the first superstep, then we only need to check the 
			// received labels
			double highestScore;
			if (getSuperstep() == 1) {
				highestScore = maxScore;
			} else {
				highestScore = labelsWithHighestScore.get(maxScoreLabel);
			}
			 
			// re-score the new label
			if (maxScoreLabel != label) {
				// delta
				highestScore -= delta / (double) getSuperstep();
			}
			// else delta = 0
			// update own label
			vertex.setValue(new LongWritable(maxScoreLabel));

			// send out messages
			for (Edge<LongWritable, DoubleWritable> e: vertex.getEdges()){
				LongWritable neighbor = e.getTargetVertexId();
				sendMessage(neighbor, new LabelWithScoreWritable(maxScoreLabel, 
						highestScore * vertex.getEdgeValue(neighbor).get() * 
						Math.pow(vertex.getNumEdges(), preference)));
			}
			
			receivedLabelsWithScores.clear();
			labelsWithHighestScore.clear();
		}
		else {
			vertex.voteToHalt();
		}
	}
}