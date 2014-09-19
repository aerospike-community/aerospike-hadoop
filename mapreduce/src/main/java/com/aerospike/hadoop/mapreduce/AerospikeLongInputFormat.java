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

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConfigurable;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class AerospikeLongInputFormat
	extends AerospikeInputFormat<LongWritable, LongWritable>
	implements JobConfigurable {

	public void configure(JobConf conf) {
	}

	protected static class AerospikeLongRecordReader
		extends AerospikeRecordReader<LongWritable> {

		public AerospikeLongRecordReader() throws IOException {
			super();
		}

		public AerospikeLongRecordReader(AerospikeSplit split) throws IOException {
			super(split);
		}

		@Override
		public LongWritable createValue() {
			return new LongWritable();
		}

		@Override
		protected LongWritable setCurrentValue(LongWritable oldApiValue,
																					 LongWritable newApiValue,
																					 Object object) {

			Long val = new Long((Integer) object);

			if (oldApiValue == null) {
				oldApiValue = new LongWritable();
				oldApiValue.set(val);
			}

			if (newApiValue != null) {
				newApiValue.set(val);
			}
			return oldApiValue;
		}
	}

	// ---------------- NEW API ----------------

	public RecordReader<LongWritable, LongWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context)
		throws IOException, InterruptedException {
		return new AerospikeLongRecordReader();
	}

	// ---------------- OLD API ----------------
  
	public org.apache.hadoop.mapred.RecordReader<LongWritable, LongWritable>
		getRecordReader(org.apache.hadoop.mapred.InputSplit split,
										JobConf jobconf,
										Reporter reporter)
		throws IOException {
		return new AerospikeLongRecordReader((AerospikeSplit) split);
	}

}
