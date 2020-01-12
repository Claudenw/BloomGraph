package org.xenei.bloomgraph.bloom;

import org.junit.runner.RunWith;
import org.xenei.bloom.multidimensional.ContainerImpl;
import org.xenei.bloom.multidimensional.Container;
import org.xenei.bloom.multidimensional.Container.Index;
import org.xenei.bloom.multidimensional.Container.Storage;
import org.xenei.bloom.multidimensional.index.FlatBloofi;
import org.xenei.bloom.multidimensional.storage.InMemory;
import org.xenei.bloomgraph.BloomTriple;
import org.xenei.junit.contract.Contract;
import org.xenei.junit.contract.ContractImpl;
import org.xenei.junit.contract.ContractSuite;
import org.xenei.junit.contract.IProducer;

import java.util.UUID;
import java.util.function.Function;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.hasher.HashFunction;
import org.apache.commons.collections4.bloomfilter.hasher.Shape;
import org.apache.commons.collections4.bloomfilter.hasher.function.Murmur128x86Cyclic;
import org.apache.jena.graph.Graph;
import org.apache.jena.mem.GraphMem;
import org.apache.jena.testing_framework.AbstractGraphProducer;

@RunWith(ContractSuite.class)
@ContractImpl(BloomGraph.class)
public class BloomGraph_CS_Test  {
    private HashFunction hashFunction = new Murmur128x86Cyclic();
    private Shape shape = new Shape( hashFunction, 3, 1.0/30000000 );

    private Function<BloomFilter,UUID> func = new Function<BloomFilter,UUID>() {

        @Override
        public UUID apply(BloomFilter bf) {
            long[] arr = bf.getBits();
            java.nio.ByteBuffer bb = java.nio.ByteBuffer.allocate(arr.length * Long.BYTES);
            bb.asLongBuffer().put(arr);
            return UUID.nameUUIDFromBytes( bb.array());
        }

    };

    private IProducer<BloomGraph> producer = new AbstractGraphProducer<BloomGraph>() {

        @Override
        public BloomGraph createNewGraph() {
            Storage<BloomTriple,UUID> storage = new InMemory<BloomTriple,UUID>();
            Index<UUID> index = new FlatBloofi<UUID>( func, shape );
            Container<BloomTriple> container = new ContainerImpl<BloomTriple,UUID>( shape, storage, index );
            return new BloomGraph(container);
        }

        @Override
        public Graph[] getDependsOn(Graph g) {
            return null;
        }

        @Override
        public Graph[] getNotDependsOn(Graph g) {
            return new Graph[] { new GraphMem() };
        }

    };

    @Contract.Inject
    public IProducer<BloomGraph> getTripleStore() {
        return producer;
    }
}
