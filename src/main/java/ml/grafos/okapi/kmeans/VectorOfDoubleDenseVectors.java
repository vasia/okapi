package ml.grafos.okapi.kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.giraph.aggregators.matrix.dense.DoubleDenseVector;
import org.apache.hadoop.io.Writable;

public class VectorOfDoubleDenseVectors implements Writable {

	private ArrayList<DoubleDenseVector> vectors;
	private int size;
	private int dimensions;
	
	public VectorOfDoubleDenseVectors(int size, int vectorDimensions) {
		this.size = size;
		this.dimensions = vectorDimensions;
		vectors = new ArrayList<DoubleDenseVector>(size);
		
		for (int i = 0; i < size; i++) {
			vectors.add(i, new DoubleDenseVector(vectorDimensions));
		}
	}

	public VectorOfDoubleDenseVectors() {}

	/**
	 * Creates a vector of vectors where only the vector at position is not null
	 * and equal to value. Used for aggregation.
	 * 
	 * @param centerId
	 * @param value
	 */
	public VectorOfDoubleDenseVectors(int size, int vectorDimensions, int centerId, DoubleDenseVector value) {
		this.size = size;
		this.dimensions = vectorDimensions;
		vectors = new ArrayList<DoubleDenseVector>(size);
		for (int i = 0; i < size; i++) {
			if ( i == centerId) {
				vectors.set(centerId, value);
			}
			else {
				vectors.set(i, null);
			}
		}
		
	}

	public ArrayList<DoubleDenseVector> getVectorList() {
		return vectors;
	}
	
	public int getSize() {
		return size;
	}
	
	public int getVectorDimensions() {
		return dimensions;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		int dimensions = in.readInt();
		for (int i = 0; i < size; ++i) {
			for (int j = 0; j < dimensions; j++) {
				getVectorList().get(i).set(j, in.readDouble());
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(size);
		out.writeInt(dimensions);
		for (int i = 0; i < size; ++i) {
			for (int j = 0; j < dimensions; j++) {
				out.writeDouble(getVectorList().get(i).get(j));
	      }
		}
	}

}
