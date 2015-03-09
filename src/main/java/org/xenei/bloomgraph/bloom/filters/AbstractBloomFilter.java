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
import java.nio.ByteOrder;
import java.util.BitSet;

import org.xenei.bloomgraph.bloom.info.ByteInfo;

/**
 * An abstract bloom filter.
 *
 * Bloom filters are based on the java BitSet class. but also track the hamming
 * value. The hamming value is tracked because we use it frequently in our
 * paging operations.
 *
 */
public abstract class AbstractBloomFilter implements BloomFilter {
	// the bitset we are using
	private final BitSet bitSet;

	// the hamming value once we have calculated it.
	private Integer hamming;

	/**
	 * Constructor
	 */
	protected AbstractBloomFilter() {
		this(new BitSet());
	}

	/**
	 * Constructor from a ByteBuffer containing a bloom filter.
	 * 
	 * @param data
	 *            the byte buffer to read filter from.
	 */
	protected AbstractBloomFilter(final ByteBuffer data) {
		this(BitSet.valueOf(data));
	}

	/**
	 * constructor from a BitSet.
	 * 
	 * @param bitSet
	 *            the bitset to read filter from.
	 */
	protected AbstractBloomFilter(final BitSet bitSet) {
		this.bitSet = bitSet;
		this.hamming = null;
	}

	/**
	 * Add a BloomFilter to this BloomFilter
	 * 
	 * @param bloomFilter
	 *            The bloomfilter to add to this filter.
	 */
	@Override
	public void add(final BloomFilter bloomFilter) {
		if (bloomFilter.getSize() != this.getSize()) {
			throw new IllegalArgumentException(
					"Bloom filters must be of the same size");
		}
		if (bloomFilter instanceof AbstractBloomFilter) {
			this.bitSet.or(((AbstractBloomFilter) bloomFilter).bitSet);
		}
		else {
			final BitSet bs = BitSet.valueOf(bloomFilter.getByteBuffer());
			this.bitSet.or(bs);
		}
		this.hamming = null;
	}

	@Override
	public void clear() {
		this.bitSet.clear();
		this.hamming = null;
	}

	@Override
	public final boolean match(final BloomFilter other) {
		long[] them = null;
		if (other instanceof AbstractBloomFilter) {
			them = ((AbstractBloomFilter) other).bitSet.toLongArray();
		}
		else {
			them = BitSet.valueOf(other.getByteBuffer()).toLongArray();
		}

		final long[] me = bitSet.toLongArray();
		if (me.length > them.length) {
			return false;
		}
		for (int i = 0; i < me.length; i++) {
			if ((me[i] & them[i]) != me[i]) {
				return false;
			}
		}
		return true;
	}

	@Override
	public final ByteBuffer getByteBuffer() {
		return ByteBuffer.wrap(bitSet.toByteArray())
				.order(ByteOrder.LITTLE_ENDIAN).asReadOnlyBuffer();
	}

	/**
	 * Return the hex value for the byte buffer.
	 */
	@Override
	public final String toString() {
		return ByteInfo.toHexString(getByteBuffer());
	}

	@Override
	public final int getHammingWeight() {
		if (hamming == null) {
			hamming = bitSet.cardinality();
		}
		return hamming;
	}

	@Override
	public final double getApproximateLog(final int depth) {
		/*
		 * this apporximation is calculated using a derivation of
		 * http://en.wikipedia.org/wiki/Binary_logarithm#Algorithm
		 */
		// the mantissa is the highest bit that is turned on.
		final int mantissa = bitSet.length() - 1;
		if (mantissa < 0) {
			// there are no bits so return 0
			return 0;
		}
		double result = mantissa;
		// now we move backwards from the highest bit until the requested
		// is achieved.
		int pos = mantissa;
		for (int i = depth; i > 0; i--) {
			pos = bitSet.previousSetBit(pos - 1);
			if (pos == -1) {
				// there are no more bits so we are done.
				return result;
			}
			// this is the exponent for the characteristic calculation.
			final double exp = pos - mantissa; // should be negative
			if (exp < -25) {
				// beyond -25 there is no detectable change in the value.
				return result;
			}
			// add the current characteristic value to the mantissa.
			result += Math.pow(2.0, exp);
		}
		return result;
	}

	/**
	 * Filter configuration class.
	 *
	 * This class contains the values for the filter configuration.
	 *
	 * @see http://hur.st/bloomfilter?n=3&p=1.0E-5
	 *
	 */
	public static class FilterConfig {
		private static final double LOG_OF_2 = Math.log(2.0);
		private static final double DENOMINATOR = Math.log(1.0 / (Math.pow(2.0,
				LOG_OF_2)));
		// number of items in the filter
		int numberOfItems;
		// probability of false positives defined as 1 in x;
		int probability;
		// number of bits in the filter;
		int numberOfBits;
		// number of hash functions
		int numberOfHashFunctions;

		/**
		 * Create a filter configuration with the specified number of bits and
		 * probability.
		 * 
		 * @param numberOfItems
		 *            Number of items to be placed in the filter.
		 * @param probability
		 *            The probability of duplicates expressed as 1 in x.
		 */
		public FilterConfig(final int numberOfItems, final int probability) {
			this.numberOfItems = numberOfItems;
			this.probability = probability;
			final double dp = 1.0 / probability;
			final Double dm = Math.ceil((numberOfItems * Math.log(dp))
					/ DENOMINATOR);
			if (dm > Integer.MAX_VALUE) {
				throw new IllegalArgumentException(
						"Resulting filter has more than " + Integer.MAX_VALUE
								+ " bits");
			}
			this.numberOfBits = dm.intValue();
			final Long lk = Math.round((LOG_OF_2 * numberOfBits)
					/ numberOfItems);
			if (lk > Integer.MAX_VALUE) {
				throw new IllegalArgumentException(
						"Resulting filter has more than " + Integer.MAX_VALUE
								+ " hash functions");
			}
			numberOfHashFunctions = lk.intValue();
		}

		/**
		 * Get the number of items that are expected in the filter. AKA: n
		 * 
		 * @return the number of items.
		 */
		public int getNumberOfItems() {
			return numberOfItems;
		}

		/**
		 * The probability of a false positive (collision) expressed as 1/x.
		 * AKA: 1/p
		 * 
		 * @return the x in 1/x.
		 */
		public int getProbability() {
			return probability;
		}

		/**
		 * The number of bits in the bloom filter. AKA: m
		 * 
		 * @return the number of bits in the bloom filter.
		 */
		public int getNumberOfBits() {
			return numberOfBits;
		}

		/**
		 * The number of hash functions used to construct the filter. AKA: k
		 * 
		 * @return the number of hash functions used to construct the filter.
		 */
		public int getNumberOfHashFunctions() {
			return numberOfHashFunctions;
		}

		/**
		 * The number of bytes in the bloom filter.
		 * 
		 * @return the number of bytes in the bloom filter.
		 */
		public int getNumberOfBytes() {
			return Double.valueOf(Math.ceil(numberOfBits / 8.0)).intValue();
		}

	}
}
