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
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.Host;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.ScanPolicy;

/**
 * An {@link InputFormat} for data stored in an Aerospike database.
 * Records are selected with an integer range.
 * Returns the record key and selected bin content as value.
 */
public abstract class AerospikeInputFormat<KK, VV> implements InputFormat<KK, VV> {

	private static String host = "127.0.0.1";
	private static int port = 3000;
	private static String namespace = "test";
    private static String setName = null;

	public static void setInputPaths(String h, int p, String ns, String sn) {
		host = h;
		port = p;
		namespace = ns;
        setName = sn;
	}

	public abstract RecordReader<KK, VV> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException;

	public InputSplit[] getSplits(JobConf job, int numSplits)
        throws IOException {

		// connect to Citrusleaf Server
        AerospikeClient client = null;
        try {
            client = new AerospikeClient(host, port);
        } catch (AerospikeException ex) {
            System.out.println(" Exception connecting to cluster; " + ex);
			System.exit(0);
        }

		if (client == null) {
			System.out.println(" Cluster " + host + ":" + port +
                               " could not be contacted.");
			System.out.println(" Unable to get names of cluster nodes.");
			System.exit(0);
			// return null;
		}
		// client.connect();
		try {
			// Sleep so that the partition hashmap is created by the client
			Thread.sleep(3000);
		} catch (Exception e) {
			System.out.println(" Exception raised when sleeping " + e);
		}

        Node[] nodes = client.getNodes();
        int nsplits = nodes.length;
		AerospikeSplit[] splits = new AerospikeSplit[nsplits];
		for (int ii = 0; ii < nsplits; ii++) {
            Node node = nodes[ii];
            String nodeName = node.getName();
            Host host = node.getHost();
            splits[ii] = new AerospikeSplit(nodeName, host.name, host.port,
                                            namespace, setName);
			// System.out.println("spilt: " + nodes.get(ii));
		}
		return splits;
	}
}
