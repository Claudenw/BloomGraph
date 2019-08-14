package org.xenei.bloomgraph;

import java.lang.ref.SoftReference;
import java.util.function.Function;

import org.apache.jena.graph.Triple;
import org.apache.jena.riot.thrift.ThriftConvert;
import org.apache.jena.riot.thrift.wire.RDF_Triple;
import org.xenei.bloomfilter.ProtoBloomFilter;
import org.xenei.bloomfilter.ProtoBloomFilterBuilder;

public class BloomTriple {
	
	public static Function<BloomTriple,ProtoBloomFilter> FUNC =
			new Function<BloomTriple,ProtoBloomFilter>(){

				@Override
				public ProtoBloomFilter apply(BloomTriple bt) {
					return bt.getProto();
				}};
				
	private final RDF_Triple triple;
	private transient SoftReference<ProtoBloomFilter> proto;
	private transient Integer hashCode;

	public BloomTriple(Triple t)
	{
		triple = ThriftConvert.convert(t, false );
		proto = null;
	}

	public BloomTriple(RDF_Triple t)
	{
		triple = t;
		proto = null;
	}
	
	public RDF_Triple getTriple() {
		return triple;
	}

	public ProtoBloomFilter getProto() {
		if (proto == null || proto.get() == null)
		{
			ProtoBloomFilterBuilder builder = new ProtoBloomFilterBuilder();
			if (! triple.S.isSetAny())
			{
				builder.update( triple.S.toString() );
			}
			if ( ! triple.P.isSetAny() )
			{
				builder.update( triple.P.toString());
			}
			if ( ! triple.O.isSetAny() )
			{
				builder.update( triple.O.toString());
			}
			proto = new SoftReference<ProtoBloomFilter>(builder.build());
		}
		return proto.get();
	}
	
	@Override
	public boolean equals( Object o ) {
		if ( o instanceof BloomTriple )
		{
			return triple.equals( ((BloomTriple) o).triple);
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		if (hashCode == null)
		{
			hashCode = triple.toString().hashCode();
		}
		return hashCode;
	}
	
}
