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
package org.xenei.bloomgraph;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.hp.hpl.jena.graph.FrontsTriple;
import com.hp.hpl.jena.graph.Triple;

/**
 * A serializable triple.
 * 
 * A serializable triple comprises a hashCode, the subject, predicate, and
 * object serialized nodes and an index of the triple in the store that it was
 * retrieved from.
 *
 */
public class SerializableTriple {

	private static final int HASH_CODE_OFFSET = 0;
	private static final int IDX_OFFSET = HASH_CODE_OFFSET + Integer.BYTES;
	private static final int S_LENGTH_OFFSET = IDX_OFFSET + Integer.BYTES;
	private static final int P_LENGTH_OFFSET = S_LENGTH_OFFSET + Integer.BYTES;
	private static final int O_LENGTH_OFFSET = P_LENGTH_OFFSET + Integer.BYTES;
	private static final int HEADER_SIZE = O_LENGTH_OFFSET + Integer.BYTES;

	/**
	 * The ANY triple
	 */
	public static final SerializableTriple ANY;

	/**
	 * Reference to the triple.
	 */
	private transient SoftReference<Triple> triple;
	private transient ByteBuffer buffer;
	private byte[] value;

	private transient SerializableNode s;
	private transient SerializableNode p;
	private transient SerializableNode o;

	static {
		ANY = new SerializableTriple(SerializableNode.ANY,
				SerializableNode.ANY, SerializableNode.ANY);
	}

	/**
	 * Create the triple from 3 serializable nodes.
	 * 
	 * @param s
	 *            the subject.
	 * @param p
	 *            the predicate.
	 * @param o
	 *            the object.
	 */
	public SerializableTriple(SerializableNode s, SerializableNode p,
			SerializableNode o) {
		fillBuffer(s, p, o);
	}

	/**
	 * Create the triple from a standard triple
	 * 
	 * @param t
	 *            the standard triple
	 * @throws IOException
	 *             on error.
	 */
	public SerializableTriple(Triple t) throws IOException {
		s = new SerializableNode(t.getSubject());
		p = new SerializableNode(t.getPredicate());
		o = new SerializableNode(t.getObject());
		fillBuffer(s, p, o);
	}

	/**
	 * Create the triple from a bytebuffer containing a serialzied triple.
	 * 
	 * @param bytes
	 *            the bytebuffer to read.
	 */
	public SerializableTriple(ByteBuffer bytes) {
		value = new byte[bytes.limit()];
		bytes.position(0);
		bytes.get(value);
		this.triple = null;

	}

	/**
	 * Returns true if the triple contains an ANY node.
	 * 
	 * @return
	 */
	public boolean containsWild() {
		return getSubject().equals(SerializableNode.ANY)
				|| getPredicate().equals(SerializableNode.ANY)
				|| getObject().equals(SerializableNode.ANY);

	}

	/**
	 * Fills the buffer from the parameters.
	 * 
	 * @param s
	 *            the subject.
	 * @param p
	 *            the predicate.
	 * @param o
	 *            the object.
	 */
	private void fillBuffer(SerializableNode s, SerializableNode p,
			SerializableNode o) {
		// hash code lifted from triple
		int hashCode = (s.hashCode() >> 1) ^ p.hashCode() ^ (o.hashCode() << 1);
		int slen = s.getByteBuffer().limit();
		int plen = p.getByteBuffer().limit();
		int olen = o.getByteBuffer().limit();
		// int dataLen = s.getBuffer().length + p.getBuffer().length
		// + o.getBuffer().length;
		int dataLen = slen + plen + olen;
		value = new byte[dataLen + HEADER_SIZE];
		buffer = getByteBuffer();
		buffer.position(0);
		buffer.putInt(HASH_CODE_OFFSET, hashCode);
		buffer.putInt(S_LENGTH_OFFSET, slen);
		buffer.putInt(P_LENGTH_OFFSET, plen);
		buffer.putInt(O_LENGTH_OFFSET, olen);
		buffer.position(HEADER_SIZE);
		// buffer.put(s.getBuffer());
		// buffer.put(p.getBuffer());
		// buffer.put(o.getBuffer());\
		buffer.put((ByteBuffer) s.getByteBuffer().position(0));
		buffer.put((ByteBuffer) p.getByteBuffer().position(0));
		buffer.put((ByteBuffer) o.getByteBuffer().position(0));
		this.s = s;
		this.p = p;
		this.o = o;
	}

	/**
	 * Get the byte buffer for this triple
	 * 
	 * @return the byte buffer.
	 */
	public ByteBuffer getByteBuffer() {
		if (buffer == null) {
			buffer = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
		}
		return buffer;
	}

	/**
	 * Get the index for the triple.
	 * 
	 * @return The index of the triple within the data store.
	 */
	public int getIndex() {
		return getByteBuffer().getInt(IDX_OFFSET);
	}

	/**
	 * Set the index for the triple.
	 * 
	 * @param idx
	 *            the index to set the triple to.
	 */
	public void setIndex(int idx) {
		getByteBuffer().putInt(IDX_OFFSET, idx);
	}

	/**
	 * Get the subject
	 * 
	 * @return The subject node
	 */
	public SerializableNode getSubject() {
		if (s == null) {
			ByteBuffer buff = getByteBuffer();
			buff.position(S_LENGTH_OFFSET);
			byte[] sBuff = new byte[buff.getInt()];
			buff.position(HEADER_SIZE);
			buff.get(sBuff);
			s = new SerializableNode(sBuff);
		}
		return s;
	}

	/**
	 * Get the predicate
	 * 
	 * @return the predicate node.
	 */
	public SerializableNode getPredicate() {
		if (p == null) {
			ByteBuffer buff = getByteBuffer();
			buff.position(S_LENGTH_OFFSET);
			int offset = HEADER_SIZE + buff.getInt(); // read s_length
			byte[] pBuff = new byte[buff.getInt()]; // read p_length
			getByteBuffer().position(offset);
			getByteBuffer().get(pBuff);
			p = new SerializableNode(pBuff);
		}
		return p;
	}

	/**
	 * Get the object.
	 * 
	 * @return the object node.
	 */
	public SerializableNode getObject() {
		if (o == null) {
			ByteBuffer buff = getByteBuffer();
			buff.position(S_LENGTH_OFFSET);
			int offset = HEADER_SIZE + buff.getInt() + buff.getInt(); // read
																		// s_length
			// &&
			// p_lenfth
			byte[] pBuff = new byte[buff.getInt()]; // read o_length
			getByteBuffer().position(offset);
			getByteBuffer().get(pBuff);
			o = new SerializableNode(pBuff);
		}
		return o;
	}

	/**
	 * Get the triple as a triple.
	 * 
	 * @return
	 * @throws IOException
	 */
	public Triple getTriple() throws IOException {
		Triple retval = null;
		if (triple != null) {
			retval = triple.get();
		}
		if (retval == null) {
			retval = new Triple(getSubject().getNode(), getPredicate()
					.getNode(), getObject().getNode());
			triple = new SoftReference<Triple>(retval);
		}
		return retval;
	}

	/**
	 * Get the hashcode. this is equivalent to the hashcode of the underlying
	 * triple.
	 */
	@Override
	public int hashCode() {
		return getByteBuffer().getInt(HASH_CODE_OFFSET);
	}

	/**
	 * Get the size of the buffer.
	 * 
	 * @return
	 */
	public int getSize() {
		return value.length;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof SerializableTriple) {
			SerializableTriple cn = (SerializableTriple) o;
			if (hashCode() == cn.hashCode() && getSize() == cn.getSize()) {
				if (getSize() > 0) {
					cn.getByteBuffer().position(HEADER_SIZE);
					getByteBuffer().position(HEADER_SIZE);
					int i = getByteBuffer().compareTo(cn.getByteBuffer());
					if (i == 0) {
						return true;
					}
				}
			}
		}
		return false;
	}

}
