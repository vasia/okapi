/**
 * Copyright 2014 Grafos.ml
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ml.grafos.okapi.semimetric.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * a pair of an LRUMapWritable and a boolean flag
 */
public class LRUMapWritableBooleanPair implements Writable {

	boolean canHalt;
	LRUMapWritable map;
	
	public LRUMapWritableBooleanPair() {}

	public LRUMapWritableBooleanPair(boolean flag, LRUMapWritable lruMap) {
		this.canHalt = flag;
		this.map = lruMap;
	}

	public boolean canHalt() {
		return canHalt;
	}
	
	public LRUMapWritable getMap() {
		return map;
	}
	
	public void setMap(LRUMapWritable other) {
		this.map = other;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		canHalt = input.readBoolean();
		map.readFields(input);
		
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeBoolean(canHalt);
		map.write(output);
	}
}
