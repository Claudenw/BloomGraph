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

import java.io.IOException;
import java.util.NoSuchElementException;

import org.xenei.bloomgraph.bloom.filters.BloomFilter;

import com.hp.hpl.jena.util.iterator.ClosableIterator;

/**
 * An abstract ID Iterator.
 *
 * There are a number of places where bloom filters are associated with IDs.
 * This abstract iterator handles matching the filter on success returning the
 * ID.
 * 
 * @param <F>
 *            The filter type to iterate over.
 * @param <T>
 *            The index type for F.
 */
public abstract class AbstractIDIterator<F extends BloomFilter, T extends AbstractIndex<F>>
		implements ClosableIterator<Integer> {
	// The target.
	private final F target;
	// the inner iterator
	private final ClosableIterator<T> inner;
	// the next value
	private Integer next;

	/**
	 * Constructor.
	 * 
	 * @param target
	 *            target to match (null = match all)
	 * @param inner
	 *            the Iterator of AbstractIndexes to wrap.
	 * @throws IOException
	 *             on error.
	 */
	protected AbstractIDIterator(final F target, final ClosableIterator<T> inner)
			throws IOException {
		this.target = target;
		this.inner = inner;
		this.next = null;
	}

	// find the next match.
	private Integer findNext() {
		while (inner.hasNext()) {
			final T candidate = inner.next();
			if ((target == null) || target.match(candidate.getFilter())) {
				return Integer.valueOf(candidate.getId());
			}
		}
		return null;
	}

	@Override
	public final boolean hasNext() {
		if (next == null) {
			next = findNext();
		}
		return next != null;
	}

	@Override
	public final Integer next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		try {
			return next;
		} finally {
			next = null;
		}
	}

	@Override
	public void close() {
		if (inner != null) {
			inner.close();
		}
	}
}
