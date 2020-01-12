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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.junit.Before;
import org.junit.Test;
import org.xenei.bloom.multidimensional.Container;
import org.xenei.bloomgraph.BloomTriple;


public abstract class AbstractBloomGraphTest {

    BloomGraph graph;
    Container<BloomTriple> bloomCollection;

    public AbstractBloomGraphTest() {
    }

    protected abstract Container<BloomTriple> makeCollection() throws IOException;

    @Before
    public void setup() throws Exception {
        bloomCollection = makeCollection();

        graph = new BloomGraph( bloomCollection );
    }

    @Test
    public void testInsert() {
        final Triple t = new Triple(
                NodeFactory.createURI("http://example.text/s"),
                NodeFactory.createURI("http://example.text/p"),
                NodeFactory.createURI("http://example.text/o"));
        graph.add(t);
        final ExtendedIterator<Triple> iter = graph.find(t);
        assertTrue(iter.hasNext());
        final Triple t2 = iter.next();
        assertEquals(t, t2);
        assertFalse(iter.hasNext());
    }

    @Test
    public void testFindWild() {
        final Node s1 = NodeFactory.createURI("http://example.text/s1");
        final Node p1 = NodeFactory.createURI("http://example.text/p1");
        final Node o1 = NodeFactory.createURI("http://example.text/o1");
        final Node s2 = NodeFactory.createURI("http://example.text/s2");
        final Node p2 = NodeFactory.createURI("http://example.text/p2");
        final Node o2 = NodeFactory.createURI("http://example.text/o2");
        final Triple t1 = new Triple(s1, p1, o1);
        final Triple t2 = new Triple(s2, p2, o2);
        final Triple t3 = new Triple(s2, p1, o1);

        graph.add(t1);
        graph.add(t2);
        graph.add(t3);

        ExtendedIterator<Triple> iter = graph.find(t1);
        assertTrue(iter.hasNext());
        Triple t = iter.next();
        assertEquals(t1, t);
        assertFalse(iter.hasNext());

        t = new Triple(s2, p1, Node.ANY);
        iter = graph.find(t);
        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals(t3, t);
        assertFalse(iter.hasNext());

        t = new Triple(s2, Node.ANY, Node.ANY);
        iter = graph.find(t);
        assertTrue(iter.hasNext());
        List<Triple> ts = iter.toList();
        assertEquals(2, ts.size());
        assertTrue(ts.contains(t2));
        assertTrue(ts.contains(t3));

        t = new Triple(Node.ANY, Node.ANY, Node.ANY);
        iter = graph.find(t);
        assertTrue(iter.hasNext());
        ts = iter.toList();
        assertEquals(3, ts.size());
        assertTrue(ts.contains(t2));
        assertTrue(ts.contains(t3));
        assertTrue(ts.contains(t1));
    }

    @Test
    public void testDelete() {
        final Node s1 = NodeFactory.createURI("http://example.text/s1");
        final Node p1 = NodeFactory.createURI("http://example.text/p1");
        final Node o1 = NodeFactory.createURI("http://example.text/o1");
        final Node s2 = NodeFactory.createURI("http://example.text/s2");
        final Node p2 = NodeFactory.createURI("http://example.text/p2");
        final Node o2 = NodeFactory.createURI("http://example.text/o2");
        final Triple t1 = new Triple(s1, p1, o1);
        final Triple t2 = new Triple(s2, p2, o2);
        final Triple t3 = new Triple(s2, p1, o1);

        //CollectionStats cs = bloomCollection.getStats();
        graph.add(t1);
        graph.add(t2);
        graph.add(t3);

        graph.delete(t2);

        //		assertEquals( 1, bloomCollection.getStats().getDeleteCount());
        //		assertEquals( 3, bloomCollection.getStats().getInsertCount());
        //		assertEquals( 4, bloomCollection.getStats().getTxnCount());
        //		assertEquals( 2, bloomCollection.getStats().getFilterCount());

        ExtendedIterator<Triple> iter = graph.find(t1);
        assertTrue(iter.hasNext());
        Triple t = iter.next();
        assertEquals(t1, t);
        assertFalse(iter.hasNext());

        t = new Triple(s2, p1, Node.ANY);
        iter = graph.find(t);
        assertTrue(iter.hasNext());
        assertEquals(t3, iter.next());
        assertFalse(iter.hasNext());

        t = new Triple(s2, Node.ANY, Node.ANY);
        iter = graph.find(t);
        assertTrue(iter.hasNext());
        assertEquals(t3, iter.next());
        assertFalse(iter.hasNext());

        t = new Triple(Node.ANY, Node.ANY, Node.ANY);
        iter = graph.find(t);
        assertTrue(iter.hasNext());
        final List<Triple> ts = iter.toList();
        assertEquals(2, ts.size());
        assertTrue(ts.contains(t3));
        assertTrue(ts.contains(t1));
    }
}
