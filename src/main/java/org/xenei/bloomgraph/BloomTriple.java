package org.xenei.bloomgraph;

import java.lang.ref.SoftReference;
import java.util.function.Function;

import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.hasher.DynamicHasher;
import org.apache.commons.collections4.bloomfilter.hasher.Murmur128;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.thrift.ThriftConvert;
import org.apache.jena.riot.thrift.wire.RDF_Triple;


public class BloomTriple {

	public static Function<BloomTriple,Hasher> FUNC =
			new Function<BloomTriple,Hasher>(){

				@Override
				public Hasher apply(BloomTriple bt) {
					return bt.getHasher();
				}};

	private final RDF_Triple triple;
	private transient SoftReference<Hasher> hasher;
	private transient Integer hashCode;

	public BloomTriple(Triple t)
	{
		triple = ThriftConvert.convert(t, false );
		hasher = null;
	}

	public BloomTriple(RDF_Triple t)
	{
		triple = t;
		hasher = null;
	}

	public RDF_Triple getTriple() {
		return triple;
	}

	public Hasher getHasher() {
		if (hasher == null || hasher.get() == null)
		{
		    Hasher.Builder builder = DynamicHasher.Factory.DEFAULT.useFunction( Murmur128.NAME );
			if (! triple.S.isSetAny())
			{
				builder.with( triple.S.toString() );
			}
			if ( ! triple.P.isSetAny() )
			{
				builder.with( triple.P.toString());
			}
			if ( ! triple.O.isSetAny() )
			{
				builder.with( triple.O.toString());
			}
			hasher = new SoftReference<Hasher>(builder.build());
		}
		return hasher.get();
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
