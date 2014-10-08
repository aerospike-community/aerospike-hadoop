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

package com.aerospike.hadoop.examples.intsuminput;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.aerospike.hadoop.mapreduce.AerospikeConfigUtil;
import com.aerospike.hadoop.mapreduce.AerospikeInputFormat;
import com.aerospike.hadoop.mapreduce.AerospikeKey;
import com.aerospike.hadoop.mapreduce.AerospikeRecord;

public class IntSumInput extends Configured implements Tool {

	private static final Log log = LogFactory.getLog(IntSumInput.class);

	private static String binName;

  public static class TokenizerMapper 
       extends Mapper<AerospikeKey, AerospikeRecord, IntWritable, LongWritable> {
    
    private final static IntWritable one = new IntWritable(1);
		private LongWritable val = new LongWritable();
      
    public void map(AerospikeKey key, AerospikeRecord rec, Context context)
			throws IOException, InterruptedException {
			val.set(new Long((Integer) rec.bins.get(binName)));
			context.write(one, val);
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<IntWritable,LongWritable,IntWritable,LongWritable> {

    private LongWritable result = new LongWritable();

    public void reduce(IntWritable key, Iterable<LongWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
      result.set(sum);
      context.write(key, result);
    }
  }

	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();

		@SuppressWarnings("deprecation")
		final Job job = new Job(conf, "AerospikeIntSumInput");

		binName = AerospikeConfigUtil.getInputBinName(conf);

		log.info("run starting");

    job.setJarByClass(IntSumInput.class);
    job.setInputFormatClass(AerospikeInputFormat.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(LongWritable.class);

		FileOutputFormat.setOutputPath(job, new Path(args[0]));

		int status = job.waitForCompletion(true) ? 0 : 1;
		log.info("run finished, status=" + status);
		return status;
	}

	public static void main(final String[] args) throws Exception {
		System.exit(ToolRunner.run(new IntSumInput(), args));
	}
}
