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
package org.xenei.bloomgraph.bloom.mem;

import java.util.NoSuchElementException;

import org.xenei.bloomgraph.bloom.index.AbstractIndex;

import com.hp.hpl.jena.util.iterator.ClosableIterator;

/**
 * An abstract index iterator.
 *
 * @param <T>
 *            The concrete implementation of AbstractIndex.
 */
public abstract class AbstractIndexIterator<T extends AbstractIndex<?>>
		implements ClosableIterator<T> {

	// the next index
	private T next;

	// true if the iterator is closed.
	private boolean closed;

	/**
	 * Create an iterator.
	 */
	public AbstractIndexIterator() {
		this.closed = false;
		next = null;
	}

	/**
	 * Find the next Index.
	 * 
	 * @return the next index.
	 */
	protected abstract T findNext();

	@Override
	public final boolean hasNext() {
		if (closed) {
			throw new IllegalStateException("Iterator is already closed");
		}
		if (next == null) {
			next = findNext();
		}
		return next != null;
	}

	@Override
	public final T next() {
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
		closed = true;
	}

}
