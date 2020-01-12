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

import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.hasher.HashFunction;
import org.apache.commons.collections4.bloomfilter.hasher.Shape;
import org.apache.commons.collections4.bloomfilter.hasher.function.Murmur128x86Cyclic;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.junit.Test;
import org.xenei.bloom.multidimensional.storage.InMemory;
import org.xenei.bloom.multidimensional.Container;
import org.xenei.bloom.multidimensional.Container.Index;
import org.xenei.bloom.multidimensional.Container.Storage;
import org.xenei.bloom.multidimensional.ContainerImpl;
import org.xenei.bloom.multidimensional.index.FlatBloofi;
import org.xenei.bloomgraph.BloomTriple;
import org.xenei.geoname.GeoName;

public class BloomGraphBigLoadTest extends AbstractBigLoadTest {

    private HashFunction hashFunction = new Murmur128x86Cyclic();


    private Function<BloomFilter,UUID> func = new Function<BloomFilter,UUID>() {

        @Override
        public UUID apply(BloomFilter bf) {
            long[] arr = bf.getBits();
            java.nio.ByteBuffer bb = java.nio.ByteBuffer.allocate(arr.length * Long.BYTES);
            bb.asLongBuffer().put(arr);
            return UUID.nameUUIDFromBytes( bb.array());
        }

    };

    public BloomGraphBigLoadTest() {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
    }

    @Override
    protected final Graph getGraph() throws Exception {
        Shape shape = new Shape( hashFunction, 3, 1.0/30000000 );
        Storage<BloomTriple,UUID> storage = new InMemory<BloomTriple,UUID>();
        Index<UUID> index = new FlatBloofi<UUID>( func,shape );
        Container<BloomTriple> container = new ContainerImpl<BloomTriple,UUID>( shape, storage, index );
        return new BloomGraph( container );
    }

    @Test
    public void x() throws Exception {
        setup();
        loadData( 80 );
        for (GeoName g : sample )
        {
            Map<String,Triple> data = parse( g );
            for (Triple triple :data.values()) {
                assertTrue( "Missing "+triple, graph.contains( triple) );
                assertTrue( "Find object failed "+triple, graph.find( triple.getSubject(), triple.getPredicate(), Node.ANY).hasNext());
                assertTrue( "Find predicate failed "+triple, graph.find( triple.getSubject(), Node.ANY, triple.getObject()).hasNext());
            }
        }


    }
}
