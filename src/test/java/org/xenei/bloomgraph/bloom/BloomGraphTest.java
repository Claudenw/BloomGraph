package org.xenei.bloomgraph.bloom;


import java.util.UUID;
import java.util.function.Function;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.hasher.HashFunction;
import org.apache.commons.collections4.bloomfilter.hasher.Shape;
import org.apache.commons.collections4.bloomfilter.hasher.function.Murmur128x86Cyclic;
import org.xenei.bloom.multidimensional.Container;
import org.xenei.bloom.multidimensional.ContainerImpl;
import org.xenei.bloom.multidimensional.Container.Index;
import org.xenei.bloom.multidimensional.Container.Storage;
import org.xenei.bloom.multidimensional.index.FlatBloofi;
import org.xenei.bloom.multidimensional.storage.InMemory;
import org.xenei.bloomgraph.BloomTriple;

public class BloomGraphTest extends AbstractBloomGraphTest {

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

    @Override
    protected Container<BloomTriple> makeCollection() {
        Shape shape = new Shape( hashFunction, 3, 1.0/30000000 );
        Storage<BloomTriple,UUID> storage = new InMemory<BloomTriple,UUID>();
        Index<UUID> index = new FlatBloofi<UUID>( func, shape );
        return new ContainerImpl<BloomTriple,UUID>( shape, storage, index );
    }

}
