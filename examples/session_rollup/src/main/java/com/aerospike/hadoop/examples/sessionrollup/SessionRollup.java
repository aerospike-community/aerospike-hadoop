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

package com.aerospike.hadoop.examples.sessionrollup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.StringTokenizer;
import java.util.UUID;

import org.json.JSONObject;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

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
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.aerospike.hadoop.mapreduce.AerospikeConfigUtil;
import com.aerospike.hadoop.mapreduce.AerospikeInputFormat;
import com.aerospike.hadoop.mapreduce.AerospikeKey;
import com.aerospike.hadoop.mapreduce.AerospikeRecord;

public class SessionRollup extends Configured implements Tool {

	private static final Log log = LogFactory.getLog(SessionRollup.class);

	private static String binName;

	private static final long SESSION_GAP_MSEC = 20 * 60 * 1000;

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, LongWritable, LongWritable> {

		// Sample line format:
		// 37518 - - [16/Jun/1998:02:48:36 +0000] "GET /images/hm_hola.gif HTTP/1.0" 200 2240

    String logEntryRegex = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\S+)";
		Pattern pat = Pattern.compile(logEntryRegex);

		DateTimeFormatter dateTimeParser =
			DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z");

		int mapcount = 0;

		public void map(LongWritable key,
										Text rec,
										OutputCollector<LongWritable, LongWritable> output,
										Reporter reporter
										) throws IOException {

			try {
				String line = rec.toString();
				Matcher matcher = pat.matcher(line);
				if (!matcher.matches() || 7 != matcher.groupCount()) {
					throw new RuntimeException("match failed on: " + line);
				}
				long userid = Long.parseLong(matcher.group(1));
				String tstamp = matcher.group(4);
				DateTime datetime = dateTimeParser.parseDateTime(tstamp);
				long msec = datetime.getMillis();
				output.collect(new LongWritable(userid), new LongWritable(msec));
			}
			catch (Exception ex) {
				// log.error("exception in map: " + ex);
			}
		}
	}

	public static class Reduce
		extends MapReduceBase
		implements Reducer<LongWritable, LongWritable, Text, Text> {
				
		public void reduce(LongWritable userid,
											 Iterator<LongWritable> tstamps,
											 OutputCollector<Text, Text> output,
											 Reporter reporter
											 ) throws IOException {

			// Copy the iterator to an array.
			ArrayList<LongWritable> tsarray = new ArrayList<LongWritable>();
			while (tstamps.hasNext())
				tsarray.add(new LongWritable(tstamps.next().get()));

			// Sort the timestamps.
			Collections.sort(tsarray);

			// Scan the array looking for session boundaries.
			long t0 = 0;
			long session_start = 0;
			long session_end = 0;
			int session_hits = 0;
			for (LongWritable tstamp: tsarray) {
				long tt = tstamp.get();

				// How long since the prior hit?
				long delta = tt - t0;

				// Is this a new session?
				if (delta > SESSION_GAP_MSEC) {

					// Is there a prior session?
					if (session_start != 0)
						collect_session(userid.get(), session_start, session_end,
														session_hits, output);

					// Reset for the new session.
					session_start = tt;
					session_hits = 0;
				}

				// Extend the current session.
				session_hits += 1;
				session_end = tt;

				// On to the next hit ...
				t0 = tt;
			}

			// Write out the last session.
			if (session_start != 0)
				collect_session(userid.get(), session_start, session_end,
												session_hits, output);
		}

		private void collect_session(long userid, long start, long end, int nhits,
																 OutputCollector<Text, Text> output
																 ) throws IOException {
			JSONObject sessobj = new JSONObject();
			sessobj.put("userid", userid);
			sessobj.put("start", start);
			sessobj.put("end", end);
			sessobj.put("nhits", nhits);
			String sessid = UUID.randomUUID().toString();
			String sessjson = sessobj.toString();
			output.collect(new Text(sessid), new Text(sessjson));
		}
	}

	public int run(final String[] args) throws Exception {

		log.info("run starting");

		final Configuration conf = getConf();

		JobConf job = new JobConf(conf, SessionRollup.class);
		job.setJobName("AerospikeSessionRollup");

		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		// job.setCombinerClass(Reduce.class);  // Reduce changes format.
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormat(TextOutputFormat.class);

		for (int ii = 0; ii < args.length - 1; ++ii) {
			FileInputFormat.addInputPath(job, new Path(args[ii]));
		}

		FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));

		JobClient.runJob(job);

		log.info("finished");
		return 0;
	}

	public static void main(final String[] args) throws Exception {
		System.exit(ToolRunner.run(new SessionRollup(), args));
	}
}
