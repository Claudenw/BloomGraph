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
package org.xenei.bloomgraph.bloom;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.xenei.bloomgraph.bloom.page.AbstractPage;
import org.xenei.bloomgraph.bloom.page.PageSearchItem;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.util.iterator.ClosableIterator;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

/**
 * The bloom IO interface.
 *
 */
public interface BloomIO {
	/**
	 * Get the graph statistics.
	 * 
	 * @return The graph statistics.
	 */
	public GraphStatistics getStatistics();

	/**
	 * Get the specified page.
	 * 
	 * @param pageNo
	 *            The page to retrieve
	 * @return The page of bloom filters.
	 * @throws IOException
	 *             on error.
	 */
	public AbstractPage getPage(int pageNo) throws IOException;

	/**
	 * Get the number of pages.
	 * 
	 * @return The number of pages.
	 * @throws IOException
	 *             on error.
	 */
	public int getPageCount() throws IOException;

	/**
	 * Get the page index origin. Some storage systems have a page 0 others
	 * start at 1. This method must return that value.
	 * 
	 * @return the index origina for hte page count,
	 */
	public int getPageIndexOrigin();

	/**
	 * Search the page for the candidate. The candidate may contain wildcards.
	 * 
	 * @param candidate
	 *            the item to search for.
	 * @return An iterator on matching items.
	 * @throws IOException
	 *             on error.
	 */
	public ExtendedIterator<Triple> find(final PageSearchItem candidate)
			throws IOException;

	/**
	 * Get an approximate count of the number of entries that will match the
	 * candidate. The candidate may contain wildcards.
	 * 
	 * @param candidate
	 *            The candidate to match.
	 * @return an estimate of the number of items that will match.
	 * @throws IOException
	 *             on error.
	 */
	public long count(final PageSearchItem candidate) throws IOException;

	/**
	 * Add the candidate to the store. The implementation does not have to
	 * verify that the item is not already stored as the calling system handles
	 * that check. The candidate will not contain wildcards.
	 * 
	 * @param candidate
	 *            The item to write to the store.
	 * @throws IOException
	 *             on error.
	 */
	public void add(final PageSearchItem candidate) throws IOException;

	/**
	 * Delete all instances of the candidate from the store. The candidate may
	 * contain wild cards.
	 * 
	 * @param candidate
	 *            the candidate to delete.
	 * @throws IOException
	 */
	public void delete(final PageSearchItem candidate) throws IOException;

	/**
	 * An iterator of iterators. This iterator will retrieve an iterator and
	 * then iterate over its objects before retrieving then next iterator. This
	 * iterator is slightly different from the standard ones provided by Jena.
	 * This iterator will not start the inner iterators until they are needed.
	 */
	static class IterIter implements ClosableIterator<Triple> {
		// the iterator we are iterating over.
		private Iterator<? extends Iterator<Triple>> outer;
		// the iterator retrieved from the outer iterator
		private Iterator<Triple> inner;

		/**
		 * Create an iterator iterator.
		 * 
		 * @param outer
		 *            The iterator of iterator we will iterate over.
		 */
		public IterIter(Iterator<? extends Iterator<Triple>> outer) {
			if (outer == null) {
				throw new IllegalArgumentException("iterator may not be null");
			}
			this.outer = outer;
			this.inner = WrappedIterator.emptyIterator();
		}

		/**
		 * Create an iterator iterator.
		 * 
		 * @param outer
		 *            The iterator of iterator we will iterate over.
		 */
		public IterIter(ExtendedIterator<ExtendedIterator<Triple>> outer) {
			if (outer == null) {
				throw new IllegalArgumentException("iterator may not be null");
			}
			this.outer = outer;
			this.inner = WrappedIterator.emptyIterator();
		}

		@Override
		public boolean hasNext() {
			if (outer == null) {
				return false;
			}
			while ((!inner.hasNext()) && outer.hasNext()) {
				if (inner instanceof ClosableIterator) {
					((ClosableIterator<?>) inner).close();
				}
				inner = outer.next();
			}
			if (inner.hasNext()) {
				return true;
			}
			close();
			return false;
		}

		@Override
		public Triple next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			return inner.next();
		}

		@Override
		public void close() {
			if (inner != null && inner instanceof ClosableIterator) {
				((ClosableIterator<?>) inner).close();
				inner = null;
			}
			if (outer != null && outer instanceof ClosableIterator) {
				((ClosableIterator<?>) outer).close();
				outer = null;
			}

		}

	}
}
