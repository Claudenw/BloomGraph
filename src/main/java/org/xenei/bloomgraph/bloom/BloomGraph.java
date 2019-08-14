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
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.jena.atlas.iterator.ActionCount;
import org.apache.jena.graph.Capabilities;
import org.apache.jena.graph.GraphStatisticsHandler;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.riot.thrift.ThriftConvert;
import org.apache.jena.riot.thrift.wire.RDF_Triple;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.bloomfilter.collections.BloomCollection;
import org.xenei.bloomgraph.BloomTriple;



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
	private final BloomCollection<BloomTriple> bloomCollection;

	/**
	 * The statistics for the graph
	 */
	private final GraphStatistics statistics;

	/**
	 * Create a bloom graph on an IO implementation.
	 * 
	 * @param bloomCollection
	 */
	public BloomGraph(final BloomCollection<BloomTriple> bloomCollection) {
		this.bloomCollection = bloomCollection;
		this.statistics = new GraphStatistics() {

			@Override
			public long getStatistic(Node S, Node P, Node O) {
				
				BloomTriple bTriple = new BloomTriple( new Triple( S, P, O) );
				if (bloomCollection.matches(bTriple.getProto()))
				{
					ActionCount<BloomTriple> ac = new ActionCount<BloomTriple>();
					bloomCollection.getCandidates( bTriple.getProto() )
					
							.forEachRemaining( ac);
					return ac.getCount();						
				}
				return 0;
			}

			@Override
			public long size() {
				return bloomCollection.size();
			}};
		
	}

	@Override
	protected final GraphStatisticsHandler createStatisticsHandler() {
		return statistics;
	}
	
	private static class MatchTriple implements Predicate<RDF_Triple> {

		RDF_Triple target;
		public MatchTriple(RDF_Triple target)
		{
			this.target = target;
		}
		
		@Override
		public boolean test(RDF_Triple arg0) {
			return ( target.S.isSetAny() || arg0.S.equals(target.S))
					&&
					( target.P.isSetAny() || arg0.P.equals(target.P))
					&&
					( target.O.isSetAny() || arg0.O.equals(target.O));
		}
		
	}

	@Override
	protected final ExtendedIterator<Triple> graphBaseFind(final Triple t) {
		
		
			BloomTriple bTriple = new BloomTriple( t );
			if (bloomCollection.matches(bTriple.getProto()))
			{
				return bloomCollection.getCandidates( bTriple.getProto() )
						.mapWith( bt -> bt.getTriple())
						.filterKeep( new MatchTriple( bTriple.getTriple() ))
						.mapWith( ThriftConvert::convert );
			}
			return WrappedIterator.emptyIterator();
	}

	@Override
	public final void performAdd(final Triple t) {
		LOG.debug("Adding triple {}", t);
		BloomTriple bTriple = new BloomTriple( t );
		bloomCollection.add( bTriple.getProto(), bTriple);
	}

	@Override
	public final void performDelete(final Triple t) {
		LOG.debug("Deleting triple {}", t);
		BloomTriple bTriple = new BloomTriple( t );
		bloomCollection.remove( bTriple.getProto(), bTriple);
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
			return true;
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

	
}
