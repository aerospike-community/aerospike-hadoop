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

package com.aerospike.spark.examples;

import java.io.IOException;

import java.nio.ByteBuffer;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Hex;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.util.Progressable;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;

import com.aerospike.hadoop.mapreduce.AerospikeConfigUtil;
import com.aerospike.hadoop.mapreduce.AerospikeOutputFormat;
import com.aerospike.hadoop.mapreduce.AerospikeRecordWriter;
import com.aerospike.hadoop.mapreduce.AerospikeLogger;

public class SparkSessionRollup {

    public static final String appName = "spark_session_rollup";
    public static final String master = "spark://as0:7077";

    public static class ExtractHits
        implements PairFunction<String, Long, Long> {

        // Sample line format:
        // 37518 - - [16/Jun/1998:02:48:36 +0000] "GET /images/hm_hola.gif HTTP/1.0" 200 2240
        final String logEntryRegex = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\S+)";
        final Pattern pat = Pattern.compile(logEntryRegex);

        final SimpleDateFormat dateTimeParser =
            new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");

        public Tuple2<Long, Long> call(String line) {

            Matcher matcher = pat.matcher(line);
            if (!matcher.matches() || 7 != matcher.groupCount())
                return new Tuple2<Long, Long>(0L, 0L);
                        
            long userid = Long.parseLong(matcher.group(1));
            String tstamp = matcher.group(4);
            ParsePosition pos = new ParsePosition(0);
            Date date = dateTimeParser.parse(tstamp, pos);
            long msec = date.getTime();

            return new Tuple2<Long, Long>(userid, msec);
        }
    }

    private static class Session {
        public long userid;
        public long start;
        public long end;
        public int nhits;

        public Session(long userid, long start, long end, int nhits) {
            this.userid = userid;
            this.start = start;
            this.end = end;
            this.nhits = nhits;
        }
    }

    public static class FindSessions
        implements PairFlatMapFunction<Tuple2<Long, Iterable<Long>>,
                                       String, Session> {

        private static final long SESSION_GAP_MSEC = 20 * 60 * 1000;

        public List<Tuple2<String, Session>>
            call(Tuple2<Long, Iterable<Long>> tup) {

            List<Tuple2<String, Session>> results =
                new ArrayList<Tuple2<String, Session>>();

            // Copy the iterator to an array.
            ArrayList<Long> tsarray = new ArrayList<Long>();
            for (Long val : tup._2())
                tsarray.add(val);
            
            // Sort the timestamps.
            Collections.sort(tsarray);

            // Scan the array looking for session boundaries.
            long t0 = 0;
            long session_start = 0;
            long session_end = 0;
            int session_hits = 0;
            for (Long tstamp: tsarray) {
                long tt = tstamp;

                // How long since the prior hit?
                long delta = tt - t0;

                // Is this a new session?
                if (delta > SESSION_GAP_MSEC) {

                    // Is there a prior session?
                    if (session_start != 0)
                        collect_session(tup._1(), session_start, session_end,
                                        session_hits, results);

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
                collect_session(tup._1(), session_start, session_end,
                                session_hits, results);

            return results;
        }

        private void collect_session(long userid, long start,
                                     long end, int nhits,
                                     List<Tuple2<String, Session>> results) {

            try {
                // Generate a sessionid from the hash of the userid and start.
                MessageDigest md = MessageDigest.getInstance("SHA-256");
                md.update(ByteBuffer.allocate(8).putLong(userid).array());
                md.update(ByteBuffer.allocate(8).putLong(start).array());
                String sessid = Hex.encodeHexString(md.digest()).substring(0,16);

                Session session = new Session(userid, start, end, nhits);

                results.add(new Tuple2<String, Session>(sessid, session));
            }
            catch (NoSuchAlgorithmException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public static class SessionOutputFormat
        extends AerospikeOutputFormat<String, Session> {

        public static class SessionRecordWriter
            extends AerospikeRecordWriter<String, Session> {

            public SessionRecordWriter(Configuration cfg,
                                       Progressable progressable) {
                super(cfg, progressable);
            }

            @Override
            public void writeAerospike(String sessid,
                                       Session session,
                                       AerospikeClient client,
                                       WritePolicy writePolicy,
                                       String namespace,
                                       String setName) throws IOException {
                Key kk = new Key(namespace, setName, sessid.toString());
                Bin bin0 = new Bin("userid", session.userid);
                Bin bin1 = new Bin("start", session.start);
                Bin bin2 = new Bin("end", session.end);
                Bin bin3 = new Bin("nhits", session.nhits);
                client.put(writePolicy, kk, bin0, bin1, bin2, bin3);
            }
        }

        public RecordWriter<String, Session>
            getAerospikeRecordWriter(Configuration conf, Progressable prog) {
            return new SessionRecordWriter(conf, prog);
        }
    }

    public static void main(String[] args) {
        com.aerospike.client.Log.setCallback(new AerospikeLogger());
        com.aerospike.client.Log.setLevel(com.aerospike.client.Log.Level.DEBUG);
        
        SparkConf conf = new SparkConf()
            .setAppName(appName)
            .set("spark.executor.memory", "2g")
            .setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.addJar("build/libs/spark_session_rollup.jar");

        JavaRDD<String> entries = sc.textFile("hdfs://localhost:54310/tmp");

        JavaPairRDD<Long, Iterable<Long>> userhits =
            entries.mapToPair(new ExtractHits()).groupByKey();

        JavaPairRDD<String, Session> sessions =
            userhits.flatMapToPair(new FindSessions());

        System.err.println(sessions.count());

        JobConf job = new JobConf();
        job.setOutputKeyClass(String.class);
        job.setOutputValueClass(Session.class);
        job.setOutputFormat(SessionOutputFormat.class);

        AerospikeConfigUtil.setOutputHost(job, "localhost");
        AerospikeConfigUtil.setOutputPort(job, 3000);
        AerospikeConfigUtil.setOutputNamespace(job, "test");
        AerospikeConfigUtil.setOutputSetName(job, "sessions3");

        sessions.saveAsHadoopDataset(job);
    }
}

// Local Variables:
// mode: java
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: nil
// End:
// vim: softtabstop=4:shiftwidth=4:expandtab
