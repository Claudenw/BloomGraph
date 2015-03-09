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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.bloomgraph.bloom.BloomIO;
import org.xenei.bloomgraph.bloom.GraphStatistics;
import org.xenei.bloomgraph.bloom.index.PageIndex;
import org.xenei.bloomgraph.bloom.page.AbstractPage;
import org.xenei.bloomgraph.bloom.page.PageSearchItem;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.Map1;
import com.hp.hpl.jena.util.iterator.NiceIterator;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

/**
 * A memory implementation of BloomIO.
 *
 */
public class MemIO implements BloomIO {
	private static final Logger LOG = LoggerFactory.getLogger(MemIO.class);

	// the list of memory pages.
	private final List<MemPage> pages;
	// the page index.
	private final PageIndexList pageIndex;
	// the statistics.
	private final BloomGraphStatistics statistics;

	/**
	 * Constructor.
	 */
	public MemIO() {
		this.pages = new ArrayList<MemPage>();
		this.pageIndex = new PageIndexList(pages);
		this.statistics = new BloomGraphStatistics();
	}

	@Override
	public GraphStatistics getStatistics() {
		return statistics;
	}

	@Override
	public AbstractPage getPage(final int i) {
		return pages.get(i);
	}

	@Override
	public int getPageCount() {
		return pages.size();
	}

	@Override
	public int getPageIndexOrigin() {
		return 0;
	}

	@Override
	public ExtendedIterator<Triple> find(final PageSearchItem candidate)
			throws IOException {
		// create an iterator with mappings to create triples.
		// we iterate over the pages that might have matching triples
		// create triple iterators from the pages
		final ExtendedIterator<ExtendedIterator<Triple>> inner = pageIndex
				.iterator(candidate).mapWith(
						new Map1<PageIndex, ExtendedIterator<Triple>>() {

							@Override
							public ExtendedIterator<Triple> map1(
									final PageIndex o) {
								try {
									return pages.get(o.getId()).find(candidate);
								} catch (final IOException e) {
									LOG.error(e.getMessage(), e);
									return NiceIterator.emptyIterator();
								}
							}
						});
		// since we have an iterator of iterators so return the iterator.
		return WrappedIterator.create(new IterIter(inner));
	}

	/**
	 * Get an approximate count of the number of entries that will match the
	 * candidate.
	 * 
	 * @param candidate
	 * @return
	 * @throws IOException
	 */
	@Override
	public long count(final PageSearchItem candidate) throws IOException {
		final Iterator<PageIndex> iter = pageIndex.iterator(candidate);
		long retval = 0;
		while (iter.hasNext()) {
			retval += pages.get(iter.next().getId()).count(candidate);
		}
		return retval;
	}

	@Override
	public final void add(final PageSearchItem candidate) throws IOException {
		LOG.debug("Adding triple {}", candidate);

		// find a page to write the data to
		AbstractPage page = null;
		if (pages.isEmpty()) {
			page = createPage();
		}
		else {
			page = pages.get(pages.size() - 1);
		}
		if (!page.write(candidate)) {
			// could not write so create a new page an try again.
			page = createPage();
			if (!page.write(candidate)) {
				throw new IllegalStateException(
						"Unable to write to newly constructed page");
			}
		}
	}

	/* for future use */
	private void lock() {
	}

	private void unlock() {
	};

	private void flush() {
	};

	/**
	 * Create a page.
	 * 
	 * @return the Page.
	 */
	private AbstractPage createPage() {
		LOG.debug("Creating new page");
		lock();
		try {
			// int id = pages.size();
			// PageIndex pageIndex = new PageIndex( id );
			final MemPage page = new MemPage(pageIndex.nextIndex());
			pages.add(page);
			flush();
			return page;
		} finally {
			unlock();
		}

	}

	@Override
	public final void delete(final PageSearchItem candidate) {
		LOG.debug("Deleting candidate {}", candidate);

		try {
			final ExtendedIterator<PageIndex> inner = pageIndex
					.iterator(candidate);
			while (inner.hasNext()) {
				try {
					pages.get(inner.next().getId()).delete(candidate);
				} catch (final IOException e) {
					LOG.error(
							String.format("Error while deleting: %s",
									e.getMessage()), e);
				}
			}
		} catch (final IOException e) {
			LOG.error(
					String.format("Error while deleting: %s", e.getMessage()),
					e);
		}

	}

	/**
	 * A class that implements the graph statistics.
	 */
	private class BloomGraphStatistics implements GraphStatistics {

		@Override
		public long getStatistic(final Node S, final Node P, final Node O) {

			try {
				return count(new PageSearchItem(new Triple(S, P, O)));
			} catch (final IOException e) {
				LOG.warn(e.getMessage(), e);
				return -1;
			}
		}

		@Override
		public long size() {
			long size = 0;
			for (final AbstractPage p : pages) {
				size += p.size();
				if (size < 0) {
					// wrapped past Long.MAX_VALUE
					return Long.MAX_VALUE;
				}
			}
			return size;
		}

		@Override
		public int pages() {
			return pages.size();
		}

	}
}
