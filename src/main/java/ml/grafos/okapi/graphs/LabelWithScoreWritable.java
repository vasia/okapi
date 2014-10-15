package ml.grafos.okapi.graphs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

	public class LabelWithScoreWritable implements Writable {

		private long label;
		private double score;
		
		public LabelWithScoreWritable() {};
		
		public LabelWithScoreWritable(long label, double score){
			this.label =label;
			this.score = score;
		}
		
		public void setScore(double score) {
			this.score = score;
		}
		
		public long getLabel() {
			return this.label;
		}
		
		public double getScore() {
			return this.score;
		}
		
		@Override
		public void readFields(DataInput input) throws IOException {
			label = input.readLong();
			score = input.readDouble();
		}

		@Override
		public void write(DataOutput output) throws IOException {
			output.writeLong(label);
			output.writeDouble(score);
		}
	}