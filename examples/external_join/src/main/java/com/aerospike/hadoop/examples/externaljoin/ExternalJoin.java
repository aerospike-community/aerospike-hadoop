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

package com.aerospike.hadoop.examples.externaljoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.hadoop.mapreduce.AerospikeConfigUtil;
import com.aerospike.hadoop.mapreduce.AerospikeOutputFormat;
import com.aerospike.hadoop.mapreduce.AerospikeRecordWriter;

public class ExternalJoin extends Configured implements Tool {

    private static final Log log = LogFactory.getLog(ExternalJoin.class);

    private static final long SESSION_GAP_MSEC = 20 * 60 * 1000;

    // Sample line format:
    // 37518 - - [16/Jun/1998:02:48:36 +0000] \
    // "GET /images/hm_hola.gif HTTP/1.0" 200 2240

    private static final String logEntryRegex = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\S+)";
    private static final Pattern pat = Pattern.compile(logEntryRegex);

    private static final SimpleDateFormat dateTimeParser =
        new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");

    public static class Map extends MapReduceBase implements
        Mapper<LongWritable, Text, LongWritable, LongWritable> {

        int mapcount = 0;

        public void map(LongWritable key,
                        Text rec,
                        OutputCollector<LongWritable, LongWritable> output,
                        Reporter reporter) throws IOException {
            try {
                String line = rec.toString();
                Matcher matcher = pat.matcher(line);
                if (!matcher.matches() || 7 != matcher.groupCount()) {
                    throw new RuntimeException("match failed on: " + line);
                }
                long userid = Long.parseLong(matcher.group(1));
                String tstamp = matcher.group(4);
                ParsePosition pos = new ParsePosition(0);
                Date date = dateTimeParser.parse(tstamp, pos);
                long msec = date.getTime();
                output.collect(new LongWritable(userid), new LongWritable(msec));
            }
            catch (Exception ex) {
                // log.error("exception in map: " + ex);
            }
        }
    }

    private static class Session implements Writable {
        public long userid;
        public long start;
        public long end;
        public int nhits;
        public int age;
        public int isMale;

        public Session(long userid, long start, long end, int nhits, int age, int isMale) {
            this.userid = userid;
            this.start = start;
            this.end = end;
            this.nhits = nhits;
            this.age = age;
            this.isMale = isMale;
        }

        public void readFields(DataInput in) throws IOException {
            userid = in.readLong();
            start = in.readLong();
            end = in.readLong();
            nhits = in.readInt();
            age = in.readInt();
            isMale = in.readInt();
        }

        public void write(DataOutput out) throws IOException {
            out.writeLong(userid);
            out.writeLong(start);
            out.writeLong(end);
            out.writeInt(nhits);
            out.writeInt(age);
            out.writeInt(isMale);
        }
    }

    public static class Reduce
        extends MapReduceBase
        implements Reducer<LongWritable, LongWritable, Text, Session> {

        private Policy policy;
        private AerospikeClient client;
        private String namespace;
        private String setName;

        @Override
        public void configure(JobConf job) {
            String host = AerospikeConfigUtil.getInputHost(job);
            int port = AerospikeConfigUtil.getInputPort(job);

            policy = new Policy();
            policy.totalTimeout = 10000;
            client = new AerospikeClient(host, port);

            namespace = AerospikeConfigUtil.getInputNamespace(job);
            setName = AerospikeConfigUtil.getInputSetName(job);
        }
                
        @Override
        public void close() {
            client.close();
        }

        public void reduce(LongWritable userid,
                           Iterator<LongWritable> tstamps,
                           OutputCollector<Text, Session> output,
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

        private void collect_session(long userid, long start,
                                     long end, int nhits,
                                     OutputCollector<Text, Session> output)
            throws IOException {

            Key kk = new Key(namespace, setName, userid);
            Record rec = client.get(policy, kk);

            int age = (Integer) rec.bins.get("age");
            int isMale = (Integer) rec.bins.get("isMale");

            try {
                // Generate a sessionid from the hash of the userid and start.
                MessageDigest md = MessageDigest.getInstance("SHA-256");
                md.update(ByteBuffer.allocate(8).putLong(userid).array());
                md.update(ByteBuffer.allocate(8).putLong(start).array());
                String sessid = Hex.encodeHexString(md.digest()).substring(0,16);

                Session session =
                    new Session(userid, start, end, nhits, age, isMale);

                output.collect(new Text(sessid), session);
            }
            catch (NoSuchAlgorithmException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public static class SessionOutputFormat
        extends AerospikeOutputFormat<Text, Session> {

        public static class SessionRecordWriter
            extends AerospikeRecordWriter<Text, Session> {

            public SessionRecordWriter(Configuration cfg,
                                       Progressable progressable) {
                super(cfg);
            }

            @Override
            public void writeAerospike(Text sessid,
                                       Session session,
                                       AerospikeClient client,
                                       WritePolicy writePolicy,
                                       String namespace,
                                       String setName) throws IOException {
                writePolicy.totalTimeout = 10000;
                Key kk = new Key(namespace, setName, sessid.toString());
                Bin bin0 = new Bin("userid", session.userid);
                Bin bin1 = new Bin("start", session.start);
                Bin bin2 = new Bin("end", session.end);
                Bin bin3 = new Bin("nhits", session.nhits);
                Bin bin4 = new Bin("age", session.age);
                Bin bin5 = new Bin("isMale", session.isMale);
                client.put(writePolicy, kk, bin0, bin1, bin2, bin3, bin4, bin5);
            }
        }

        public RecordWriter<Text, Session>
            getAerospikeRecordWriter(Configuration conf, Progressable prog) {
            return new SessionRecordWriter(conf, prog);
        }
    }

    public int run(final String[] args) throws Exception {

        log.info("run starting");

        final Configuration conf = getConf();

        JobConf job = new JobConf(conf, ExternalJoin.class);
        job.setJobName("AerospikeExternalJoin");

        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        // job.setCombinerClass(Reduce.class);  // Reduce changes format.
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Session.class);

        job.setOutputFormat(SessionOutputFormat.class);

        for (int ii = 0; ii < args.length; ++ii)
            FileInputFormat.addInputPath(job, new Path(args[ii]));

        JobClient.runJob(job);

        log.info("finished");
        return 0;
    }

    public static void main(final String[] args) throws Exception {
        System.exit(ToolRunner.run(new ExternalJoin(), args));
    }
}

// Local Variables:
// mode: java
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: nil
// End:
// vim: softtabstop=4:shiftwidth=4:expandtab
