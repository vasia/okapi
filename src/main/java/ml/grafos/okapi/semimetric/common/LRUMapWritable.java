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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 * Extends Hadoop's MapWritable to provide a fixed-size
 * LRU MapWritable.
 * The values of the map are timestamps, corresponding to access time
 * of the keys.
 * When a new key-value pair is to be added, if the map is full,
 * the new key-value pair replaces the one with the oldest timestamp.
 * Otherwise, the key-value pair is added with the current timestamp. 
 *
 * To avoid unexpected behavior, only the provided putLRU and removeLRUKey 
 * should be used and not the default put/remove methods.
 */
public class LRUMapWritable extends org.apache.hadoop.io.MapWritable {
  
	private int size;
	private int currentSize;

	public LRUMapWritable(){}
	public LRUMapWritable(int s) {
		super();
		this.size = s;
		this.currentSize = 0;
	}
	
	/**
	 * Adds the given key to the LRUMap.
	 * If the map is full, the least recently used element is replaced.
	 * 
	 */
	public void putLRU(LongLongPair key) {
		if (this.isFull()) {
			if (this.containsKey(key)) {
				// update the element's timestamp
				this.put(key, new LongWritable(System.currentTimeMillis()));
			}
			else {
				// replace the least recently used element
				this.removeLRUKey();
				this.put(key, new LongWritable(System.currentTimeMillis()));
				currentSize++;
			}
		}
		else {
			if (!this.containsKey(key)) {
				currentSize++;
			}
			this.put(key, new LongWritable(System.currentTimeMillis()));
		}
	}

	@Override
	public boolean containsKey(Object key) {
		for (Entry<Writable, Writable> entry : this.entrySet()) {
			if (((LongLongPair) entry.getKey()).equals((LongLongPair)key)) {
				return true;
			}
		}
		return false;
	};
	
	/**
	 * Removes the key with the oldest timestamp
	 */
	private void removeLRUKey() {
		long oldestTimeStamp = System.currentTimeMillis();
		LongLongPair keyToReplace = null;
		for (Entry<Writable, Writable> entry : this.entrySet()) {
			if (((LongWritable)(entry.getValue())).get() < oldestTimeStamp) {
				oldestTimeStamp = ((LongWritable)(entry.getValue())).get();
				keyToReplace = (LongLongPair) entry.getKey();
			}
		}
		if (keyToReplace != null) {
			this.remove(keyToReplace);
			currentSize--;
		}
	}

	public int getSize() {
		return this.size;
	}

	private boolean isFull() {
		return (currentSize == size);
	}

  @Override
  public String toString() {
    StringBuilder s = new StringBuilder();
    for (Entry<Writable, Writable> e : entrySet()) {
      s.append("("+e.getKey()+","+e.getValue()+")");
    }
    return s.toString();
  }
}
