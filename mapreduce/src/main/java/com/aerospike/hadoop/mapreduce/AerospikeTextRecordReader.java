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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.RecordReader;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;

public class AerospikeTextRecordReader
	extends RecordReader<LongWritable, Text>
	implements org.apache.hadoop.mapred.RecordReader<LongWritable, Text> {

	private static final Log log =
		LogFactory.getLog(AerospikeTextRecordReader.class);

	private ASSCanReader in;

	private LinkedBlockingQueue<Record> queue = new LinkedBlockingQueue<Record>();
	private boolean isScanFinished = false;
	private boolean isError = false;
	private boolean isScanRunning = false;
	private String binName;
	
	public class CallBack implements ScanCallback {
		@Override
		public void scanCallback(Key key, Record record) throws AerospikeException {
			try {
				queue.put(record);
			} catch (Exception ex) {
				throw new AerospikeException("exception in queue.put", ex);
			}
		}
	}

	public class ASSCanReader extends java.lang.Thread {

		String node;
		String host;
		int port;
		String namespace;
		String setName;

		ASSCanReader(String node, String host, int port,
								 String ns, String setName) {
			this.node = node;
			this.host = host;
			this.port = port;
			this.namespace = ns;
			this.setName = setName;
		}

		public void run() {
			try {
				AerospikeClient client = new AerospikeClient(host, port);
				try {
					ScanPolicy scanPolicy = new ScanPolicy();
					CallBack cb = new CallBack();
					log.info("scan starting");
					isScanRunning = true;
					client.scanNode(scanPolicy, node, namespace, setName, cb);
					isScanFinished = true;
					log.info("scan finished");
				}
				finally {
					client.close();
				}
			}
			catch (Exception ex) {
				isError = true;
				return;
			}
		}
	}

	public AerospikeTextRecordReader(AerospikeSplit split)
		throws IOException {
		final String node = split.getNode();
		final String host = split.getHost();
		final int port = split.getPort();
		final String namespace = split.getNameSpace();
		final String setName = split.getSetName();
		this.binName = split.getBinName();
		in = new ASSCanReader(node, host, port, namespace, setName);
		in.start();
		log.info("node: " + node);
	}

	public LongWritable createKey() {
		return new LongWritable();
	}

	public Text createValue() {
		return new Text();
	}

	public synchronized boolean next(LongWritable key, Text value)
		throws IOException {
		final int waitMSec = 1000;
		int trials = 5;

		try {
			Record rec;
			while (true) {
				if (isError)
					return false;
				
				if (!isScanRunning) {
					Thread.sleep(100);
					continue;
				}
			
				if (!isScanFinished && queue.size() == 0) {
					if (trials == 0) {
						log.error("SCAN TIMEOUT");
						return false;
					}
					log.info("queue empty: waiting...");
					Thread.sleep(waitMSec);
					trials--;
				} else if (isScanFinished && queue.size() == 0) {
					return false;
				} else if (queue.size() != 0) {
					rec = queue.take();
					break;
				}
				
			}
			
			key.set(1);
			String val = (String) rec.bins.get(binName);
			value.set(val);
			// log.info("next: " + val);
		}
		catch (Exception ex) {
			throw new IOException("exception in AerospikeTextRecordReader.next", ex);
		}
		return true;
	}

	public float getProgress() {
		if (isScanFinished)
			return 1.0f;
		else
			return 0.0f;
	}

	public synchronized long getPos() throws IOException {
		return 0;
	}

	public synchronized void close() throws IOException {
		if (in != null) {
			try {
				in.join();
			}
			catch (Exception ex) {
				throw new IOException("exception in AerospikeTextRecordReader.close",
															ex);
			}
			in = null;
		}
	}
}
