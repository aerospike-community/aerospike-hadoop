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

package com.aerospike.hadoop.examples.aggregateintinput;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

public class AggregateIntInput extends Configured implements Tool {

	private static final Log log = LogFactory.getLog(AggregateIntInput.class);

	private static final int KK = 3163;

	private static final String binName = "bin1";

  public static class Map
		extends Mapper<AerospikeKey, AerospikeRecord, LongWritable, LongWritable> {

		private LongWritable val = new LongWritable();
		private LongWritable mod = new LongWritable();
      
    public void map(AerospikeKey key, AerospikeRecord rec, Context context
										) throws IOException, InterruptedException {
			int vv = (Integer) rec.bins.get(binName);
			val.set(vv);
			mod.set(vv % KK);
			context.write(mod, val);
    }
  }

  public static class Reduce
		extends Reducer<LongWritable, LongWritable, LongWritable, Text> {

    public void reduce(LongWritable mod,
											 Iterable<LongWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

			long num = 0;	// number of elements
			long sum = 0;	// sum of elements
			long min = Long.MAX_VALUE;	// minimum element
			long max = Long.MIN_VALUE;	// maximum element

			for (LongWritable val : values) {
				long vv = val.get();
				num += 1;
				sum += vv;
				if (vv < min) min = vv;
				if (vv > max) max = vv;
			}

			String rec = String.format("%d %d %d %d", num, min, max, sum);

      context.write(mod, new Text(rec));
    }
  }

	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();

		@SuppressWarnings("deprecation")
		final Job job = new Job(conf, "AerospikeAggregateIntInput");

		log.info("run starting on bin " + binName);

    job.setJarByClass(AggregateIntInput.class);
    job.setInputFormatClass(AerospikeInputFormat.class);
    job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
    // job.setCombinerClass(Reduce.class); // no combiner
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(args[0]));

		int status = job.waitForCompletion(true) ? 0 : 1;
		log.info("run finished, status=" + status);
		return status;
	}

	public static void main(final String[] args) throws Exception {
		System.exit(ToolRunner.run(new AggregateIntInput(), args));
	}
}
