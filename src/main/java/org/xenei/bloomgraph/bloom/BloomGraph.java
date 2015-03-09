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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.bloomgraph.bloom.page.AbstractPage;
import org.xenei.bloomgraph.bloom.page.PageSearchItem;

import com.hp.hpl.jena.graph.Capabilities;
import com.hp.hpl.jena.graph.GraphStatisticsHandler;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.impl.GraphBase;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

/**
 * A graph that implements searching via BloomFilters.
 *
 */
public class BloomGraph extends GraphBase {
	private static final Logger LOG = LoggerFactory.getLogger(BloomGraph.class);

	/**
	 * the bloom IO implementation. IO can be implemented on a number of storage
	 * platforms.
	 */
	private final BloomIO io;

	/**
	 * The statistics for the graph
	 */
	private final GraphStatistics statistics;

	/**
	 * Create a bloom graph on an IO implementation.
	 * 
	 * @param io
	 */
	public BloomGraph(final BloomIO io) {
		this.io = io;
		this.statistics = io.getStatistics();
	}

	@Override
	protected final GraphStatisticsHandler createStatisticsHandler() {
		return statistics;
	}

	@Override
	protected final ExtendedIterator<Triple> graphBaseFind(final TripleMatch m) {
		LOG.debug("Finding triple {}", m.asTriple());
		try {
			return io.find(new PageSearchItem(m.asTriple()));
		} catch (final IOException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	@Override
	public final void performAdd(final Triple t) {
		LOG.debug("Adding triple {}", t);
		final PageSearchItem candidate = new PageSearchItem(t);
		try {
			// check to see if it is already in the graph
			final ExtendedIterator<Triple> iter = io.find(candidate);
			try {
				if (iter.hasNext()) {
					LOG.debug("Triple already in graph");
					return;
				}
			} finally {
				iter.close();
			}
			io.add(candidate);

		} catch (final IOException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	@Override
	public final void performDelete(final Triple t) {
		LOG.debug("Deleting triple {}", t);
		final PageSearchItem candidate = new PageSearchItem(t);

		try {
			io.delete(candidate);
		} catch (final IOException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}

	}

	@Override
	protected int graphBaseSize() {
		final long size = statistics.size();
		return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
	}

	@Override
	public Capabilities getCapabilities() {
		if (capabilities == null) {
			// capabilities = new MyCapabilities(io);
			capabilities = new MyCapabilities();
		}
		return capabilities;
	}

	/**
	 * Capabilities for the bloom graph.
	 *
	 */
	private class MyCapabilities implements Capabilities {

		@Override
		public boolean sizeAccurate() {
			return false;
		}

		@Override
		public boolean addAllowed() {
			return true;
		}

		@Override
		public boolean addAllowed(final boolean every) {
			return addAllowed();
		}

		@Override
		public boolean deleteAllowed() {
			return true;
		}

		@Override
		public boolean deleteAllowed(final boolean every) {
			return deleteAllowed();
		}

		@Override
		public boolean canBeEmpty() {
			return true;
		}

		@Override
		public boolean iteratorRemoveAllowed() {
			return false;
		}

		@Override
		public boolean findContractSafe() {
			return true;
		}

		@Override
		public boolean handlesLiteralTyping() {
			return false;
		}
	}

	// ///////////////////////
	/**
	 * A debug statement to display debug information for a specific page.
	 * 
	 * @param i
	 *            the page to dump debug info for.
	 */
	public void debugPage(final int i) {
		try {
			final AbstractPage p = io.getPage(i);
			p.debug(String.format("Graph page %s debug", i));
		} catch (final IOException e) {
			LOG.warn("Unable to retrieve page " + 1, e);
		}
	}
}
