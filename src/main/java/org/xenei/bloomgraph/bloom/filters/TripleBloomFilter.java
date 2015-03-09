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
 * A bloom filter for a triple
 */
public class TripleBloomFilter extends AbstractBloomFilter {
	// 3 nodes in the filter, 1 in 100000 collisions
	public static final FilterConfig CONFIG = new FilterConfig(3, 100000);

	/**
	 * Verify that a byte buffer has enough bytes.
	 * 
	 * @param data
	 * @return the byte buffer.
	 * @throws IllegalArgumentException
	 *             if the wrong size.
	 */
	private static ByteBuffer verifyDataLength(final ByteBuffer data) {
		if (data.limit() < CONFIG.getNumberOfBytes()) {
			throw new IllegalArgumentException("Data buffer must be at least "
					+ CONFIG.getNumberOfBytes() + " bytes long");
		}
		if (data.limit() > CONFIG.getNumberOfBytes()) {
			return (ByteBuffer) data.slice().limit(CONFIG.getNumberOfBytes());
		}
		return data;
	}

	/**
	 * A public builder for a TripleBloomFilter.
	 */
	public static AbstractBuilder<TripleBloomFilter> BUILDER = new Builder();

	/**
	 * Construct a filter using the bitset.
	 * 
	 * @param bitSet
	 *            The bitset for the filter.
	 */
	private TripleBloomFilter(BitSet bitSet) {
		super(bitSet);
	}

	/**
	 * Create a triple bloom filter from a byte buffer.
	 * 
	 * @param buff
	 *            the buffer to read.
	 */
	public TripleBloomFilter(ByteBuffer buff) {
		super(verifyDataLength(buff));
	}

	@Override
	public int getSize() {
		return CONFIG.getNumberOfBytes();
	}

	/**
	 * The builder for the TripleBloomFilter.
	 *
	 */
	private static class Builder extends AbstractBuilder<TripleBloomFilter> {

		/**
		 * Constructor.
		 */
		private Builder() {
			super(CONFIG);
		}

		@Override
		protected TripleBloomFilter construct(final BitSet bitSet) {
			return new TripleBloomFilter(bitSet);
		}
	}
}
