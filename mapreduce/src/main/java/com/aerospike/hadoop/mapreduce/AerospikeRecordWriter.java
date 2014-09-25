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
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

public class AerospikeRecordWriter
	extends RecordWriter
	implements org.apache.hadoop.mapred.RecordWriter {

	private static final Log log = LogFactory.getLog(AerospikeRecordWriter.class);
	private static final int NO_TASK_ID = -1;

	protected final Configuration cfg;
	protected boolean initialized = false;

	private String uri;

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
	}

	protected void init() throws IOException {
		log.info("init");
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
	}
}
