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

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.ClientPolicy;

/**
 * An {@link InputFormat} for data stored in an Aerospike database.
 * Records are selected with an integer range.
 * Returns the record key and selected bin content as value.
 */
public class AerospikeInputFormat<KK, VV> implements InputFormat<KK, VV> {

	private static String host = "127.0.0.1";
	private static int port = 3000;
	private static String namespace = "test";

	public static void setInputPaths(String h, int p, String ns) {
		host = h;
		port = p;
		namespace = ns;
	}

	public abstract RecordReader<KK, VV> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException;

	public InputSplit[] getSplits(JobConf job, int numSplits)
        throws IOException {

		// connect to Citrusleaf Server
		ClientPolicy policy = new ClientPolicy();
		AerospikeClient cc = new AerospikeClient(policy, host, port);
		if (cc == null) {
			System.out.println(" Cluster " + host + ":" + port +
                               " could not be contacted.");
			System.out.println(" Unable to get names of cluster nodes.");
			System.exit(0);
			// return null;
		}
		// cc.connect();
		try {
			// Sleep so that the partition hashmap is created by the client
			Thread.sleep(3000);
		} catch (Exception e) {
			System.out.println(" Exception raised when sleeping " + e);
		}
		
		// retrieve names of citrusleaf nodes, as needed by the scanNode
		CLConnectionManager connMgr = cc.getconnMgr();
		ArrayList<String> nodes = (ArrayList<String>) connMgr.getNodeNameList();
		numSplits = nodes.size();
		
		AerospikeSplit[] splits = new AerospikeSplit[numSplits];
		for (int i = 0; i < numSplits; i++) {
			// get InetSocketAddress of the node
			CLNode node = connMgr.getNodeFromNodeName(nodes.get(i));
			InetSocketAddress ip = node.primaryAddress;
			splits[i] = new AerospikeSplit(nodes.get(i), ip.getHostName(),
                                    ip.getPort(), namespace);
			// System.out.println("spilt: " + nodes.get(i));
		}
		return splits;
	}
}
