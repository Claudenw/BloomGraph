/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.bloomgraph.bloom.index;

import java.nio.ByteBuffer;

import org.xenei.bloomgraph.bloom.filters.BloomFilter;

/**
 * An abstract class that associates a bloom filter with an index id.
 *
 * @param <T>
 *            The BloomFilter type.
 */
public abstract class AbstractIndex<T extends BloomFilter> {
	private final int id;
	private final T filter;

	/**
	 * Constructor. The filter will be empty.
	 * 
	 * @param idx
	 *            The id to associate with the filter.
	 */
	protected AbstractIndex(int idx) {
		this.filter = createFilter(null);
		this.id = idx;
	}

	/**
	 * Constructor with filter and id.
	 * 
	 * @param filter
	 *            The filter to associate
	 * @param idx
	 *            The id to associate it with.
	 */
	protected AbstractIndex(final T filter, int idx) {
		this.filter = filter;
		this.id = idx;
	}

	/**
	 * Constructor with bytebuffer and idx.
	 * 
	 * @param buff
	 *            The byte buffer to create the filter from.
	 * @param idx
	 *            the id to associate it with.
	 */
	protected AbstractIndex(final ByteBuffer buff, int idx) {
		if (buff == null) {
			throw new IllegalArgumentException("Buffer may not be null");
		}
		this.filter = createFilter(buff);
		this.id = idx;
	}

	/**
	 * Get the associated id.
	 * 
	 * @return the id
	 */
	public int getId() {
		return id;
	}

	/**
	 * Get the size of the filter + the size of the id.
	 * 
	 * @return the total size for filter and id.
	 */
	public int getSize() {
		return filter.getSize() + Integer.BYTES;
	}

	/**
	 * Delete the index. At a minimum this should reset the bloom filter value
	 * to all zeros.
	 * 
	 * @see doDelete()
	 */
	public abstract void delete();

	/**
	 * create the filter from the buffer.
	 * 
	 * @param buff
	 *            The buffer to create with.
	 * @return the created filter.
	 */
	protected abstract T createFilter(ByteBuffer buff);

	/**
	 * Get the filter.
	 * 
	 * @return the filter.
	 */
	public T getFilter() {
		return filter;
	}

	/**
	 * delete the values from the filter. Sets the filter value to zeros
	 */
	protected final void doDelete() {
		filter.clear();
	}

}
