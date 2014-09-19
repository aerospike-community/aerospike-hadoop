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

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConfigurable;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class AerospikeTextInputFormat
	extends AerospikeInputFormat<LongWritable, Text>
	implements JobConfigurable {

	public void configure(JobConf conf) {
	}

	protected static class AerospikeTextRecordReader
		extends AerospikeRecordReader<Text> {

		public AerospikeTextRecordReader(AerospikeSplit split) throws IOException {
			super(split);
		}

		@Override
		public Text createValue() {
			return new Text();
		}

		@Override
		protected Text setCurrentValue(Text oldApiValue,
																	 Text newApiValue,
																	 Object object) {
			String val = object.toString();
			if (oldApiValue == null) {
				oldApiValue = new Text();
				oldApiValue.set(val);
			}

			if (newApiValue != null) {
				newApiValue.set(val);
			}
			return oldApiValue;
		}
	}

	// ---------------- NEW API ----------------

	public RecordReader<LongWritable, Text> createRecordReader(
      InputSplit split, TaskAttemptContext context)
		throws IOException, InterruptedException {
		return new AerospikeTextRecordReader((AerospikeSplit) split);
	}

	// ---------------- OLD API ----------------
  
	public org.apache.hadoop.mapred.RecordReader<LongWritable, Text>
		getRecordReader(org.apache.hadoop.mapred.InputSplit split,
										JobConf jobconf,
										Reporter reporter)
		throws IOException {
		return new AerospikeTextRecordReader((AerospikeSplit) split);
	}

}
