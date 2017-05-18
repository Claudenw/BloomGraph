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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;

import org.xenei.bloomgraph.SerializableNode;
import org.xenei.bloomgraph.bloom.filters.MurmurHash;
import org.xenei.bloomgraph.bloom.filters.AbstractBloomFilter.FilterConfig;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

/**
 * An abstract BloomFilter buider.
 *
 * @param <T>
 *            The type of bloom filter being constructed.
 */
public abstract class AbstractBuilder<T extends AbstractBloomFilter> {
	// the filter config.
	private FilterConfig config;

	/**
	 * Constructor.
	 * 
	 * @param config
	 *            The filter configuration.
	 */
	public AbstractBuilder(FilterConfig config) {
		this.config = config;
	}

	/**
	 * The construct the resulting type from the bitset.
	 * 
	 * @param bitSet
	 *            The bitset to construct the resulting type from.
	 * @return The constructed bloom filter.
	 */
	abstract protected T construct(BitSet bitSet);

	/**
	 * Determine if a node should be included in the filter. the Node.ANY node
	 * and a blank node should not be added to the filter.
	 * 
	 * @param node
	 *            the node to test.
	 * @return true if the node should be added, false otherwise.
	 */
	private boolean shouldIndex(Node node) {
		return !(Node.ANY.equals(node) || node.isBlank());
	}

	/**
	 * Build the filter from a triple.
	 * 
	 * @param triple
	 *            The triple to create the filter for.
	 * @return the new filter.
	 * @throws IOException
	 *             on error.
	 */
	public T build(Triple triple) throws IOException {
		return build(triple.getSubject(), triple.getPredicate(),
				triple.getObject());
	}

	/**
	 * Build the filter from three nodes.
	 * 
	 * @param subject
	 *            The subject node.
	 * @param predicate
	 *            The predicate node.
	 * @param object
	 *            The object node.
	 * @return A bloom filter.
	 * @throws IOException
	 *             on error.
	 */
	public T build(Node subject, Node predicate, Node object)
			throws IOException {
		BitSet bitSet = new BitSet(config.getNumberOfBits());
		if (shouldIndex(subject)) {
			update(bitSet, subject);
		}
		if (shouldIndex(predicate)) {
			update(bitSet, predicate);
		}
		if (shouldIndex(object)) {
			update(bitSet, object);
		}
		return construct(bitSet);
	}

	/**
	 * Updates the bitset from the node.
	 * 
	 * @param bitSet
	 *            the bit set to update
	 * @param node
	 *            the node to add.
	 * @throws IOException
	 */
	private void update(BitSet bitSet, Node node) throws IOException {
		// as noted in org.apache.cassandra.utils.BloomFilter
		// Murmur is faster than an SHA-based approach and provides as-good
		// collision
		// resistance. The combinatorial generation approach described in
		// https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf
		// does prove to work in actual tests, and is obviously faster
		// than performing further iterations of murmur.
		SerializableNode serNode = new SerializableNode(node);
		ByteBuffer bb = serNode.getByteBuffer();
		long[] hash = new long[2];
		MurmurHash.hash3_x64_128(bb, 0, bb.limit(), 0L, hash);
		for (int i = 0; i < config.getNumberOfHashFunctions(); i++) {
			bitSet.set(Long.valueOf(
					Math.abs((hash[0] + (long) i * hash[1])
							% config.getNumberOfBits())).intValue());
		}
	}

}
