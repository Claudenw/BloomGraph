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

/**
 * A bloom filter definition.
 *
 */
public interface BloomFilter {

	/**
	 * Return true if this & other = this
	 * 
	 * @param other
	 *            the other bloom filter to match.
	 * @return true if they match.
	 */
	public boolean match(BloomFilter other);

	/**
	 * Get the hamming weight for this filter.
	 * 
	 * Ths is the number of bits that are on in the filter.
	 * 
	 * @return The hamming weight.
	 */
	public int getHammingWeight();

	/**
	 * Get the approximate log for this filter. If the bloom filter is
	 * considered as an unsigned number what is the approximate base 2 log of
	 * that value. The depth argument indicates how many extra bits are to be
	 * considered in the log calculation. At least one bit must be considered.
	 * If there are no bits on then the log value is 0.
	 * 
	 * @see AbstractBloomFilter.getApproximateLog()
	 * @param depth
	 *            the number of bits to consider.
	 * @return the approximate log.
	 */
	public double getApproximateLog(int depth);

	/**
	 * Add another bloom filter to this one. The result is this |= other.
	 * 
	 * @param other
	 *            The other bloom filter to merge in.
	 */
	public void add(BloomFilter other);

	/**
	 * Return the size in bytes.
	 * 
	 * @return
	 */
	public int getSize();

	/**
	 * Set the filter to all zeros
	 * 
	 * @return
	 */
	public void clear();

	/**
	 * Get the bloom filter as a byte buffer.
	 * 
	 * @return A byte buffer representing this filter.
	 */
	public ByteBuffer getByteBuffer();
}
