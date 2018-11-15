/* 
 * Copyright 2018 Aerospike, Inc.
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

package com.aerospike.hadoop.examples.generateprofiles;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.hadoop.mapreduce.AerospikeOutputFormat;
import com.aerospike.hadoop.mapreduce.AerospikeRecordWriter;

public class GenerateProfiles extends Configured implements Tool {

    private static final Log log = LogFactory.getLog(GenerateProfiles.class);

    // Sample line format:
    // 37518 - - [16/Jun/1998:02:48:36 +0000] \
    // "GET /images/hm_hola.gif HTTP/1.0" 200 2240

    private static final String logEntryRegex = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\S+)";
    private static final Pattern pat = Pattern.compile(logEntryRegex);

    private final static IntWritable one = new IntWritable(1);

    public static class Map extends MapReduceBase implements
         Mapper<LongWritable, Text, LongWritable, IntWritable> {

        int mapcount = 0;

        public void map(LongWritable key,
                        Text rec,
                        OutputCollector<LongWritable, IntWritable> output,
                        Reporter reporter) throws IOException {
            try {
                String line = rec.toString();
                Matcher matcher = pat.matcher(line);
                if (!matcher.matches() || 7 != matcher.groupCount()) {
                    throw new RuntimeException("match failed on: " + line);
                }
                long userid = Long.parseLong(matcher.group(1));
                output.collect(new LongWritable(userid), one);
            }
            catch (Exception ex) {
                // log.error("exception in map", ex);
            }
        }
    }

    private static class Profile implements Writable {
        public long userid;
        public int age;
        public int isMale;

        public Profile(long userid, int age, int isMale) {
            this.userid = userid;
            this.age = age;
            this.isMale = isMale;
        }

        public void readFields(DataInput in) throws IOException {
            userid = in.readLong();
            age = in.readInt();
            isMale = in.readInt();
        }

        public void write(DataOutput out) throws IOException {
            out.writeLong(userid);
            out.writeInt(age);
            out.writeInt(isMale);
        }
    }

    public static class Reduce
        extends MapReduceBase
        implements Reducer<LongWritable, IntWritable, LongWritable, Profile> {
                
        public void reduce(LongWritable userid,
                           Iterator<IntWritable> ones,
                           OutputCollector<LongWritable, Profile> output,
                           Reporter reporter
                           ) throws IOException {

            // Fake age based on userid.
            int age = ((int) userid.get() % 40) + 20;

            // Fake gender based on userid.
            int isMale = (int) userid.get() % 2;

            Profile profile = new Profile(userid.get(), age, isMale);
            output.collect(userid, profile);
        }
    }

    public static class ProfileOutputFormat
        extends AerospikeOutputFormat<LongWritable, Profile> {

        public static class ProfileRecordWriter
            extends AerospikeRecordWriter<LongWritable, Profile> {

            public ProfileRecordWriter(Configuration cfg,
                                       Progressable progressable) {
                super(cfg);
            }

            @Override
            public void writeAerospike(LongWritable userid,
                                       Profile profile,
                                       AerospikeClient client,
                                       WritePolicy writePolicy,
                                       String namespace,
                                       String setName) throws IOException {
                writePolicy.totalTimeout = 10000;
                Key kk = new Key(namespace, setName, userid.get());
                Bin bin0 = new Bin("userid", profile.userid);
                Bin bin1 = new Bin("age", profile.age);
                Bin bin2 = new Bin("isMale", profile.isMale);
                client.put(writePolicy, kk, bin0, bin1, bin2);
            }
        }

        public RecordWriter<LongWritable, Profile>
            getAerospikeRecordWriter(Configuration conf, Progressable prog) {
            return new ProfileRecordWriter(conf, prog);
        }
    }

    public int run(final String[] args) throws Exception {

        log.info("run starting");

        final Configuration conf = getConf();

        JobConf job = new JobConf(conf, GenerateProfiles.class);
        job.setJobName("AerospikeGenerateProfiles");

        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        // job.setCombinerClass(Reduce.class);  // Reduce changes format.
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Profile.class);

        job.setOutputFormat(ProfileOutputFormat.class);

        for (int ii = 0; ii < args.length; ++ii)
            FileInputFormat.addInputPath(job, new Path(args[ii]));

        JobClient.runJob(job);

        log.info("finished");
        return 0;
    }

    public static void main(final String[] args) throws Exception {
        System.exit(ToolRunner.run(new GenerateProfiles(), args));
    }
}

// Local Variables:
// mode: java
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: nil
// End:
// vim: softtabstop=4:shiftwidth=4:expandtab
