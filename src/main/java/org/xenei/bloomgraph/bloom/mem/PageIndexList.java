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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.xenei.bloomgraph.bloom.filters.PageBloomFilter;
import org.xenei.bloomgraph.bloom.index.PageIndex;
import org.xenei.bloomgraph.bloom.page.PageSearchItem;

import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.Filter;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

/**
 *
 * A list of page indexes and pages.
 *
 */
public class PageIndexList {

	// the list of index buffers
	private final List<PageIndex> indexBuffer;

	// the list of pages.
	private final List<MemPage> pages;

	/**
	 * Constructor.
	 * 
	 * @param pages
	 *            The pages.
	 */
	public PageIndexList(final List<MemPage> pages) {
		this.pages = pages;
		this.indexBuffer = new ArrayList<PageIndex>(2);
	}

	/**
	 * get the page at the specified id.
	 * 
	 * @param idx
	 *            the id to retrieve
	 * @return The Page index.
	 */
	public PageIndex getIndex(final int idx) {
		return indexBuffer.get(idx);
	}

	/**
	 * Get an iterator over the PageIndex objects.
	 * 
	 * @return the PageIndex iterator.
	 * @throws IOException
	 *             on error.
	 */
	public Iterator<PageIndex> iterator() throws IOException {
		return new PageIndexIterator();
	}

	/**
	 * Get an iterator of PageIndex that match the candidate.
	 * 
	 * @param candidate
	 *            The candidate to match
	 * @return the iterator of PageIndex that match the candidate filter.
	 * @throws IOException
	 *             on error.
	 */
	public ExtendedIterator<PageIndex> iterator(final PageSearchItem candidate)
			throws IOException {
		return WrappedIterator.create(iterator()).filterKeep(
				new PageIndexFilter(candidate.getPageFilter()));
	}

	/**
	 * Get the next page index.
	 */
	public PageIndex nextIndex() {
		final PageIndex retval = new PageIndex(pages.size());
		// indexBuffer.ensureCapacity(retval.getId());
		while (indexBuffer.size() < retval.getId()) {
			indexBuffer.add(null);
		}
		if (indexBuffer.size() == retval.getId()) {
			indexBuffer.add(retval);
		}
		else {
			indexBuffer.set(retval.getId(), retval);
		}
		return retval;
	}

	/**
	 * A page index iterator.
	 *
	 */
	private class PageIndexIterator extends AbstractIndexIterator<PageIndex> {
		private int pos;

		PageIndexIterator() throws IOException {
			super();
			pos = 0;
		}

		@Override
		protected PageIndex findNext() {
			if (pos < pages.size()) {
				return getIndex(pos++);
			}
			return null;
		}
	}

	/**
	 * A page index filter. Filters based on matching the target.
	 *
	 */
	private class PageIndexFilter extends Filter<PageIndex> {
		private final PageBloomFilter target;

		/**
		 * Constructor
		 * 
		 * @param target
		 *            The target to match.
		 */
		public PageIndexFilter(final PageBloomFilter target) {
			this.target = target;
		}

		@Override
		public boolean accept(final PageIndex o) {
			return target.match(o.getFilter());
		}
	}
}
