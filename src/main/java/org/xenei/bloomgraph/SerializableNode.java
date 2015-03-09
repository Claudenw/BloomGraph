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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.TypeMapper;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.impl.LiteralLabel;
import com.hp.hpl.jena.graph.impl.LiteralLabelFactory;
import com.hp.hpl.jena.rdf.model.AnonId;

/**
 * A binary representation of a Node. Includes the mapping of the node to an
 * index value. For literal node types long lexical values are compressed for
 * storage.
 * 
 * Will return the Node value. Keeps a soft reference to the node so that it may
 * be garbage collected if necessary.
 * 
 * Is serializable so that it can be written to a stream if necessary.
 * 
 */
public class SerializableNode implements NodeTypes, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 214628993540911756L;
	private static final int HASH_CODE_OFFSET = 0;
	private static final int TYPE_OFFSET = 4;
	private static final int DATA_OFFSET = 5;

	/**
	 * A node representing a serialized ANY node.
	 */
	public static final SerializableNode ANY;

	static {
		try {
			ANY = new SerializableNode(Node.ANY);
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * the node that we are storing.
	 */
	private transient SoftReference<Node> node;

	/**
	 * the buffer we read/write
	 */
	private transient ByteBuffer buffer;

	/**
	 * The byte array that actually gets written.
	 */
	private byte[] value;

	/**
	 * Create the node from the byte array.
	 * 
	 * @param serializedValue
	 *            The serialized node.
	 */
	public SerializableNode(byte[] serializedValue) {
		this.node = null;
		this.value = serializedValue;
	}

	/**
	 * Create a SerializableNode from the provided node.
	 * 
	 * Defaults to a maximum size of Integer.MAX_VALUE
	 * 
	 * @param n
	 *            The node to serialize.
	 * @throws IOException
	 *             on error.
	 */
	public SerializableNode(Node n) throws IOException {
		this(n, Integer.MAX_VALUE);
	}

	/**
	 * Create a serializable node from a node and limit the buffer size. If the
	 * serialized node is a literal and exceeds the maximum buffer size the data
	 * are compressed before writing and decompressed on reading.
	 * 
	 * @param n
	 *            the node to wrap.
	 * @param maxBlob
	 *            The maximum buffer size.
	 * @throws IOException
	 *             on error.
	 */
	public SerializableNode(Node n, int maxBlob) throws IOException {
		this.node = new SoftReference<Node>(n);
		if (n.equals(Node.ANY)) {
			fillBuffer(Node.ANY.hashCode(), _ANY, null);
		}
		else if (n.isVariable()) {
			fillBuffer(n.hashCode(), _VAR, encodeString(n.getName()));
		}
		else if (n.isURI()) {
			fillBuffer(n.hashCode(), _URI, encodeString(n.getURI()));
		}
		else if (n.isBlank()) {
			fillBuffer(n.hashCode(), _ANON, encodeString(n.getBlankNodeId()
					.getLabelString()));
		}
		else if (n.isLiteral()) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream os = new DataOutputStream(baos);
			write(os, n.getLiteralLexicalForm());
			write(os, n.getLiteralLanguage());
			write(os, n.getLiteralDatatypeURI());

			os.close();
			baos.close();
			byte[] value = baos.toByteArray();
			if (value.length > maxBlob) {
				baos = new ByteArrayOutputStream();
				GZIPOutputStream dos = new GZIPOutputStream(baos);
				dos.write(value);
				dos.close();
				fillBuffer(n.hashCode(), (byte) (_LIT | _COMPRESSED),
						baos.toByteArray());
			}
			else {
				fillBuffer(n.hashCode(), _LIT, value);
			}
		}
		else {
			throw new IllegalArgumentException("Unknown node type " + n);
		}
	}

	/**
	 * Get the byte buffer for this node.
	 * 
	 * @return the byte buffer for this node.
	 */
	public ByteBuffer getByteBuffer() {
		if (buffer == null) {
			buffer = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
		}
		return buffer;
	}

	/**
	 * Fill the byte buffer.
	 * 
	 * @param hashCode
	 *            The hash code for the node
	 * @param type
	 *            The type of the node
	 * @param buff
	 *            The data for the node.
	 */
	protected void fillBuffer(int hashCode, byte type, byte[] buff) {
		value = new byte[DATA_OFFSET + (buff == null ? 0 : buff.length)];
		((ByteBuffer) getByteBuffer().position(0)).putInt(hashCode).put(type);
		if (buff != null) {
			getByteBuffer().put(buff);
		}
	}

	/**
	 * write string to the output. Writes the length first then the data. writes
	 * -1 for a null. Data are encodes as UTF-8.
	 * 
	 * @param os
	 *            The output to write to
	 * @param s
	 *            The string to write.
	 * @throws IOException
	 */
	private void write(DataOutputStream os, String s) throws IOException {
		if (s == null) {
			os.writeInt(-1);
		}
		else {
			byte[] b = encodeString(s);
			os.writeInt(b.length);
			if (b.length > 0) {
				os.write(b);
			}
		}
	}

	/**
	 * Return just the node data
	 * 
	 * @return
	 */
	private byte[] getData() throws IOException {
		int size = getSize();
		byte[] retval = new byte[size];
		if (size > 0) {
			getByteBuffer().position(DATA_OFFSET);
			getByteBuffer().get(retval);

			if ((getType() & _COMPRESSED) == _COMPRESSED) {
				DataInputStream input = null;
				ByteArrayOutputStream output = null;
				try {
					input = new DataInputStream(new BufferedInputStream(
							new GZIPInputStream(
									new ByteArrayInputStream(retval))));
					output = new ByteArrayOutputStream();
					IOUtils.copy(input, output);
					retval = output.toByteArray();
				} finally {
					IOUtils.closeQuietly(input);
					IOUtils.closeQuietly(output);
				}
			}
		}
		return retval;
	}

	/**
	 * The size of the node data without the overhead
	 * 
	 * @return
	 */
	public int getSize() {
		return value.length - DATA_OFFSET;
	}

	/**
	 * Returns true if the node is a literal.
	 * 
	 * @return ture if the node is a literal.
	 */
	public boolean isLiteral() {
		return (getType() & NodeTypes._LIT) == NodeTypes._LIT;
	}

	public byte getType() {
		getByteBuffer().position(TYPE_OFFSET);
		return getByteBuffer().get();
	}

	/**
	 * Gets the node that is wrapped.
	 * 
	 * If the node is already known it is returned otherwise an attempt is made
	 * to deserialize the node.
	 * 
	 * @return The node.
	 * @throws IOException
	 *             if the node can not be deserialized..
	 */
	public final Node getNode() throws IOException {
		Node retval = null;
		if (node != null) {
			retval = node.get();
		}
		if (retval == null) {
			retval = extractNode();
			node = new SoftReference<Node>(retval);
		}
		return retval;
	}

	/**
	 * Get the hashCode. this is the equivalent of the hashcode for the original
	 * node.
	 */
	@Override
	public int hashCode() {
		getByteBuffer().position(HASH_CODE_OFFSET);
		return getByteBuffer().getInt();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof SerializableNode) {
			SerializableNode cn = (SerializableNode) o;
			if (hashCode() == cn.hashCode() && getType() == cn.getType()
					&& getSize() == cn.getSize()) {
				if (getSize() > 0) {
					cn.getByteBuffer().position(DATA_OFFSET);
					getByteBuffer().position(DATA_OFFSET);
					int i = getByteBuffer().compareTo(cn.getByteBuffer());
					if (i == 0) {
						return true;
					}
				}
				else {
					return true;
				}
			}
		}
		return false;
	}

	private String decodeString(byte[] b) {
		try {
			return new String(b, "UTF-8");
		} catch (UnsupportedEncodingException e) { // should not happen
			throw new RuntimeException(e);
		}
	}

	/**
	 * Read a string from the input stream.
	 * 
	 * @see write()
	 * @param is
	 *            the input stream
	 * @return the string read from the stream.
	 * @throws IOException
	 */
	private String read(DataInputStream is) throws IOException {
		int n = is.readInt();
		if (n == -1) {
			return null;
		}
		byte[] b = new byte[n];
		if (n > 0) {
			is.read(b);
		}
		return decodeString(b);
	}

	/**
	 * Extract the node from the buffer.
	 */
	protected Node extractNode() throws IOException {

		Node lnode = null;
		byte type = getType();
		switch (type & 0x0F) {
			case _ANON:
				lnode = NodeFactory.createAnon(AnonId
						.create(decodeString(getData())));
				break;

			case _LIT:
				InputStream bais = new ByteArrayInputStream(getData());
				DataInputStream is = new DataInputStream(
						new BufferedInputStream(bais));
				String lex = read(is);
				String lang = StringUtils.defaultIfBlank(read(is), null);
				String dtURI = read(is);
				is.close();
				RDFDatatype dtype = StringUtils.isEmpty(dtURI) ? null
						: TypeMapper.getInstance().getTypeByName(dtURI);
				LiteralLabel ll = LiteralLabelFactory.create(lex, lang, dtype);
				lnode = NodeFactory.createLiteral(ll);
				break;

			case _URI:
				lnode = NodeFactory.createURI(decodeString(getData()));
				break;

			case _VAR:
				lnode = NodeFactory.createVariable(decodeString(getData()));
				break;

			case _ANY:
				lnode = Node.ANY;
				break;

			default:
				throw new RuntimeException(String.format(
						"Unable to parse node: %0o", type));
		}
		return lnode;
	}

	/**
	 * Encode a string as UTF-8 bytes.
	 * 
	 * @param s
	 * @return
	 */
	private byte[] encodeString(String s) {
		try {
			return s.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) { // should not happen
			throw new RuntimeException(e);
		}
	}

}