/* 
 * Copyright 2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more
 * contributor license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.aerospike.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;

public class AerospikeSplit extends org.apache.hadoop.mapreduce.InputSplit implements InputSplit {
	private String node;
	private String host;
	private int port;
	private String namespace;
	private String setName;

	AerospikeSplit() {
	}

	public AerospikeSplit(String node, String host, int port,
                          String ns, String setName) {
		this.node = node;
		this.host = host;
		this.port = port;
		this.namespace = ns;
		this.setName = setName;
	}

	public String getNode() {
		return node;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public String getNameSpace() {
		return namespace;
	}

	public String getSetName() {
		return setName;
	}

	public long getLength() {
		return 1;
	}

	public String toString() {
		return "node:" + node + ", host:" + host + ", port:" + port + ", ns:"
            + namespace + ", setName:" + setName;
	}

	// //////////////////////////////////////////
	// Writable methods
	// //////////////////////////////////////////

	public void write(DataOutput out) throws IOException {
		Text.writeString(out, node);
		Text.writeString(out, host);
		out.writeInt(port);
		Text.writeString(out, namespace);
		Text.writeString(out, setName);
	}

	public void readFields(DataInput in) throws IOException {
		node = new String(Text.readString(in));
		host = new String(Text.readString(in));
		port = in.readInt();
		namespace = new String(Text.readString(in));
		setName = new String(Text.readString(in));
	}

	public String[] getLocations() throws IOException {
		return new String[] {};
	}
}
