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

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;

public abstract class AerospikeRecordReader<VV>
	extends RecordReader<LongWritable, VV>
	implements org.apache.hadoop.mapred.RecordReader<LongWritable, VV> {

	private static final Log log = LogFactory.getLog(AerospikeRecordReader.class);

	private ASSCanReader in;

	private LinkedBlockingQueue<Record> queue = new LinkedBlockingQueue<Record>();
	private boolean isScanFinished = false;
	private boolean isError = false;
	private boolean isScanRunning = false;
	private String binName;
	
	private LongWritable currentKey;
	private VV currentValue;

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

	public AerospikeRecordReader()
		throws IOException {
		log.info("NEW CTOR");
	}

	public AerospikeRecordReader(AerospikeSplit split)
		throws IOException {
		log.info("OLD CTOR");
		init(split);
	}

	public void init(AerospikeSplit split)
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

	// Must be provided by derived class ...
	public abstract VV createValue();

	protected LongWritable setCurrentKey(LongWritable oldApiKey,
																			 LongWritable newApiKey,
																			 long keyval) {

		if (oldApiKey == null) {
			oldApiKey = new LongWritable();
			oldApiKey.set(keyval);
		}

		// new API might not be used
		if (newApiKey != null) {
			newApiKey.set(keyval);
		}
		return oldApiKey;
	}

	// Must be provided by derived class ...
	protected abstract VV setCurrentValue(VV oldApiValue,
																				VV newApiValue,
																				Object object);

	public synchronized boolean next(LongWritable key, VV value)
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

			long nextkey = 1;
			Object nextval = rec.bins.get(binName);

			currentKey = setCurrentKey(currentKey, key, nextkey);
			currentValue = setCurrentValue(currentValue, value, nextval);

		}
		catch (Exception ex) {
			throw new IOException("exception in AerospikeRecordReader.next", ex);
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
				throw new IOException("exception in AerospikeRecordReader.close",
															ex);
			}
			in = null;
		}
	}

	// ---------------- NEW API ----------------

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
		throws IOException {
		log.info("INITIALIZE");
		init((AerospikeSplit) split);
	}

	@Override
	public boolean nextKeyValue() throws IOException {
		// new API call routed to old API
		if (currentKey == null) {
			currentKey = createKey();
		}
		if (currentValue == null) {
			currentValue = createValue();
		}

		// FIXME: does the new API mandate a new instance each time (?)
		return next(currentKey, currentValue);
	}

	@Override
	public LongWritable getCurrentKey() throws IOException {
		return currentKey;
	}

	@Override
	public VV getCurrentValue() {
		return currentValue;
	}
}
