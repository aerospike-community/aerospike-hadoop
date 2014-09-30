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

package com.aerospike.hadoop.examples.wordcountoutput;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.aerospike.hadoop.mapreduce.AerospikeOutputFormat;

// These are all needed by MyOutputFormat.
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.mapred.RecordWriter;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.hadoop.mapreduce.AerospikeRecordWriter;

public class WordCountOutput extends Configured implements Tool {

	private static final Log log = LogFactory.getLog(WordCountOutput.class);

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
				
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static class MyOutputFormat
		extends AerospikeOutputFormat<Text, IntWritable> {

		public static class MyRecordWriter
			extends AerospikeRecordWriter<Text, IntWritable> {

			public MyRecordWriter(Configuration cfg, Progressable progressable) {
				super(cfg, progressable);
			}

			@Override
			public void writeAerospike(Text key, IntWritable value,
																 AerospikeClient client, WritePolicy writePolicy,
																 String namespace, String setName,
																 String binName, String keyName
																 ) throws IOException {
				Key kk = new Key(namespace, setName, key.toString());
				Bin bin1 = new Bin(keyName, key.toString());
				Bin bin2 = new Bin(binName, value.get());
				client.put(writePolicy, kk, bin1, bin2);
			}
		}

		public RecordWriter<Text, IntWritable>
			        getAerospikeRecordWriter(Configuration conf, Progressable prog) {
			return new MyRecordWriter(conf, prog);
		}
	}

	public int run(final String[] args) throws Exception {

		log.info("run starting");

		final Configuration conf = getConf();

		JobConf job = new JobConf(conf, WordCountOutput.class);
		job.setJobName("AerospikeWordCountOutput");

    for (int ii = 0; ii < args.length; ++ii) {
      FileInputFormat.addInputPath(job, new Path(args[ii]));
    }

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setOutputFormat(MyOutputFormat.class);

		JobClient.runJob(job);

		log.info("finished");
		return 0;
	}

	public static void main(final String[] args) throws Exception {
		System.exit(ToolRunner.run(new WordCountOutput(), args));
	}
}
