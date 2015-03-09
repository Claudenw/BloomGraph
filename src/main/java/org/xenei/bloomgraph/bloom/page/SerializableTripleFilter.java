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
package org.xenei.bloomgraph.bloom.page;

import java.io.IOException;

import org.xenei.bloomgraph.SerializableNode;
import org.xenei.bloomgraph.SerializableTriple;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.util.iterator.Filter;

/**
 * A serializable triple filter.
 *
 */
public class SerializableTripleFilter extends Filter<SerializableTriple> {

	private final Triple triple;

	/**
	 * Constructor
	 * 
	 * @param candidate
	 *            the candiate with the triple to match.
	 */
	public SerializableTripleFilter(final PageSearchItem candidate) {
		this.triple = candidate.getTriple();
	}

	protected final boolean match(final Node n, final SerializableNode sn)
			throws IOException {
		return n.equals(Node.ANY) || sn.getNode().equals(n);
	}

	protected final boolean match(final Triple t, final SerializableTriple st)
			throws IOException {
		return match(t.getSubject(), st.getSubject())
				&& match(t.getPredicate(), st.getPredicate())
				&& match(t.getObject(), st.getObject());
	}

	@Override
	public boolean accept(final SerializableTriple o) {
		try {
			return match(triple, o);
		} catch (final IOException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

}