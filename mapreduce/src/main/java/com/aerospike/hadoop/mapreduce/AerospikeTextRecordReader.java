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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.RecordReader;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.ScanCallback;

public class AerospikeTextRecordReader
    implements RecordReader<LongWritable, Text> {

	private ASSCanReader in;

	private LinkedBlockingQueue<Record> queue =
        new LinkedBlockingQueue<Record>();
	private boolean isScanFinished = false;
	private boolean isError = false;
	private boolean isScanRunning = false;
	
	

	public static class Record {
		public String namespace;
		public String set;
		public byte[] digest;
		public Map<String, Object> value;
		public int generation;
		public int ttl;
		public Object user_data;

		Record(String namespace, String set, byte[] digest,
				Map<String, Object> value, int generation, int ttl,
				Object user_data) {
			this.namespace = namespace;
			this.set = set;
			this.digest = digest;
			this.value = value;
			this.generation = generation;
			this.ttl = ttl;
			this.user_data = user_data;
		}

	}

	public class CallBack implements ScanCallback {
		public void scanCallback(String namespace, String set, byte[] digest,
				Map<String, Object> value, int generation, int ttl,
				Object user_data) {
			Record rec = new Record(namespace, set, digest, value, generation,
					ttl, user_data);
			try {
				queue.put(rec);
			} catch (Exception e) {
				System.out.println("ClRecordREeader: Exception raised while inserting record into queue.");
			}
		}
	}

	/*
	private static class ScanObject {
		public int n_objects;
		public ArrayList<String> startValue;
		public ArrayList<String> startBin;
		public boolean failed;
		public boolean delay;
		public boolean nobindata;
		public int n_keys;
		public int num_bins;
		public boolean[] key_found;
	}
	*/

	public class ASSCanReader extends java.lang.Thread {
		String node;
		String host;
		int port;
		String namespace;

		ASSCanReader(String node, String host, int port, String ns) {
			this.node = node;
			this.host = host;
			this.port = port;
			this.namespace = ns;
		}

		public void run() {
			AerospikeClient cc = new AerospikeClient(host, port);
			if (cc == null) {
				System.out.println(" Cluster " + host + ":" + port + " could not be contacted.");
				System.out.println(" Unable to scan cluster nodes.");
				isError = true;
				return;
			}

			cc.connect();
			try {
				// Sleep so that the partition hashmap is created by the client
				Thread.sleep(3000);
			} catch (Exception e) {
				System.out.println(" Exception raised when sleeping " + e);
			}

            ScanPolicy scanPolicy = new ScanPolicy();

			CallBack cb = new CallBack();
			isScanRunning = true;
			cc.scanNode(scanPolicy, node, namespace, setName, cb);
			isScanFinished = true;
			// System.out.println("Scan finished");
		}
	}

	public AerospikeTextRecordReader(Configuration job, AerospikeSplit split)
        throws IOException {
		final String node = split.getNode();
		final String host = split.getHost();
		final int port = split.getPort();
		final String namespace = split.getNameSpace();
		in = new ASSCanReader(node, host, port, namespace);
		in.start();
		
		// System.out.println("node: " + node);
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
						System.out.println("Scan timeout");
						return false;
					}
					System.out.println("Queue empty: waiting...");
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
			System.out.println("next: " + rec.digest.toString() + " " +
                               rec.generation);
			value.set("" + rec.generation);
		} catch (Exception e) {
			System.out.println("Exception");
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
			} catch (Exception e) {
				System.out.println("Scan thread got interrupted.");
			}
			in = null;
		}
	}
}
