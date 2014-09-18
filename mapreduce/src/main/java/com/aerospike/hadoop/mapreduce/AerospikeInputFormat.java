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
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.Host;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.ScanPolicy;

/**
 * An {@link InputFormat} for data stored in an Aerospike database.
 */
public abstract class AerospikeInputFormat<KK, VV>
	extends InputFormat<KK, VV>
	implements org.apache.hadoop.mapred.InputFormat<KK, VV> {

	private static final Log log = LogFactory.getLog(AerospikeInputFormat.class);

	private static String host = "127.0.0.1";
	private static int port = 3000;
	private static String namespace = "test";
	private static String setName = null;
	private static String binName = null;

	public static String INPUT_SPEC = "aerospike.input.spec";

	// Handles column spec strings like: "localhost:3000:test:sample:bin1"
	//
	public static void parse_spec(String colspec) {
		String[] inparam = colspec.split(":");
		host = inparam[0];
		port = Integer.parseInt(inparam[1]);
		namespace = inparam[2];
		setName = inparam[3];
		binName = inparam[4];
		log.info(String.format("parse_spec: %s %d %s %s %s",
													 host, port, namespace, setName, binName));
	}

	// ---------------- NEW API ----------------

  public static void addInputSpec(Job job, String spec) {
		parse_spec(spec);
  }

	public List<InputSplit> getSplits(JobContext context) throws IOException {
		// Delegate to the old API.
		Configuration cfg = context.getConfiguration();
		JobConf jobconf = (cfg instanceof JobConf ? (JobConf) cfg : new JobConf(cfg));
		return Arrays.asList((InputSplit[]) getSplits(jobconf, jobconf.getNumMapTasks()));
	}

	public abstract RecordReader<KK, VV> createRecordReader(
      InputSplit split, TaskAttemptContext context)
		throws IOException, InterruptedException;

	// ---------------- OLD API ----------------

	public static void setInputPaths(String colspec) {
		parse_spec(colspec);
	}

	public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf job, int numSplits)
		throws IOException {
		try {
			log.info(String.format("using: %s %d %s %s %s",
														 host, port, namespace, setName, binName));
			AerospikeClient client = new AerospikeClient(host, port);
			try {
				Node[] nodes = client.getNodes();
				int nsplits = nodes.length;
				if (nsplits == 0) {
					throw new IOException("no Aerospike nodes found");
				}
				log.info(String.format("found %d nodes", nsplits));
				AerospikeSplit[] splits = new AerospikeSplit[nsplits];
				for (int ii = 0; ii < nsplits; ii++) {
					Node node = nodes[ii];
					String nodeName = node.getName();
					Host nodehost = node.getHost();
					splits[ii] = new AerospikeSplit(nodeName, nodehost.name, nodehost.port,
																					namespace, setName, binName);
					log.info("split: " + node);
				}
				return splits;
			}
			finally {
				client.close();
			}
		}
		catch (Exception ex) {
			throw new IOException("exception in getSplits", ex);
		}
	}

	public abstract org.apache.hadoop.mapred.RecordReader<KK, VV> getRecordReader(
      org.apache.hadoop.mapred.InputSplit split, JobConf job, Reporter reporter)
		throws IOException;

}
