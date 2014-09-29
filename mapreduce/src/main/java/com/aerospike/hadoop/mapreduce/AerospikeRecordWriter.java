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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;

public class AerospikeRecordWriter
	extends RecordWriter
	implements org.apache.hadoop.mapred.RecordWriter {

	private static final Log log = LogFactory.getLog(AerospikeRecordWriter.class);
	private static final int NO_TASK_ID = -1;

	protected final Configuration cfg;
	protected boolean initialized = false;

	private static String namespace;
	private static String setName;
	private static String binName;
	private static String keyName;
	private static AerospikeClient client;
	private static WritePolicy writePolicy;

	private Progressable progressable;

	public AerospikeRecordWriter(Configuration cfg, Progressable progressable) {
		this.cfg = cfg;
		this.progressable = progressable;
	}

	@Override
	public void write(Object key, Object value) throws IOException {
		if (!initialized) {
			initialized = true;
			init();
		}

		String keystr = key.toString();
		int valint = ((IntWritable) value).get();

		Key kk = new Key(namespace, setName, keystr);
		Bin bin1 = new Bin(keyName, keystr);
		Bin bin2 = new Bin(binName, valint);

		client.put(writePolicy, kk, bin1, bin2);
	}

	protected void init() throws IOException {

		String host = AerospikeConfigUtil.getOutputHost(cfg);
		int port = AerospikeConfigUtil.getOutputPort(cfg);

		namespace = AerospikeConfigUtil.getOutputNamespace(cfg);
		setName = AerospikeConfigUtil.getOutputSetName(cfg);
		binName = AerospikeConfigUtil.getOutputBinName(cfg);
		keyName = AerospikeConfigUtil.getOutputKeyName(cfg);

		log.info(String.format("init: %s %d %s %s %s %s",
													 host, port, namespace, setName, binName, keyName));

		ClientPolicy policy = new ClientPolicy();
		policy.user = "";
		policy.password = "";
		policy.failIfNotConnected = true;

		client = new AerospikeClient(policy, host, port);

		writePolicy = new WritePolicy();
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException {
		doClose(context);
	}

	@Override
	public void close(org.apache.hadoop.mapred.Reporter reporter
										) throws IOException {
		doClose(reporter);
	}

	protected void doClose(Progressable progressable) {
		log.info("doClose");
		initialized = false;
		if (client != null)
			client.close();
	}
}
