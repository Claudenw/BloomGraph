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
package org.xenei.bloomgraph.bloom.filters;

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * A bloom filter for a page of TripleBloomFilters
 *
 */
public class PageBloomFilter extends AbstractBloomFilter {
	// 10000 triples, 1 in 100000 collisions
	public static final FilterConfig CONFIG = new FilterConfig(10000, 100000);

	/**
	 * verify that the buffer length is proper.
	 * 
	 * @param data
	 *            the buffer to check.
	 * @return true if it is of the proper size, false otherwise.
	 */
	private static ByteBuffer verifyDataLength(final ByteBuffer data) {
		if (data.limit() > CONFIG.getNumberOfBytes()) {
			return (ByteBuffer) data.slice().limit(CONFIG.getNumberOfBytes());
		}
		return data;
	}

	/**
	 * A static builder for PageBloomFilters.
	 */
	public static AbstractBuilder<PageBloomFilter> BUILDER = new Builder();

	/**
	 * Constructor for an empyt filter.
	 */
	public PageBloomFilter() {
		super();
	}

	/**
	 * constructor for a filter from the bitset.
	 * 
	 * @param bitSet
	 *            the bitset that represents the filter.
	 */
	private PageBloomFilter(final BitSet bitSet) {
		super(bitSet);
	}

	/**
	 * constructor for a filter from a byte buffer. The byte buffer must be the
	 * proper size as required by the configuration.
	 * 
	 * @param buff
	 *            the byte buffer to load the data from.
	 */
	public PageBloomFilter(final ByteBuffer buff) {
		super(verifyDataLength(buff));
	}

	@Override
	public int getSize() {
		return CONFIG.getNumberOfBytes();
	}

	/**
	 * A builder for the PageBloomFilter.
	 *
	 */
	private static class Builder extends AbstractBuilder<PageBloomFilter> {

		/**
		 * Construct the builder from the configuration.
		 */
		private Builder() {
			super(CONFIG);
		}

		@Override
		protected PageBloomFilter construct(final BitSet bitSet) {
			return new PageBloomFilter(bitSet);
		}
	}

}
