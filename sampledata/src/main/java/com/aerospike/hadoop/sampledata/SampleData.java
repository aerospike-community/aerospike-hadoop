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

package com.aerospike.hadoop.sampledata;

import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.Record;
import com.aerospike.client.task.IndexTask;

public class SampleData {

	// aql> CREATE INDEX bin1ndx ON test.sample (bin1) NUMERIC

	private static final Log log = LogFactory.getLog(SampleData.class);

	private static String host;
	private static int port;
	private static String namespace;
	private static String setName;
	private static String binName;
	private static AerospikeClient client;
	private static WritePolicy writePolicy;

	public static void run(String[] args) throws Exception {

		int argi = 0;
		String asspec = args[argi++];
		String dataType = args[argi++];

		log.info(String.format("saw %s %s", asspec, dataType));

		String[] inparam = asspec.split(":");
		host = inparam[0];
		port = Integer.parseInt(inparam[1]);
		namespace = inparam[2];
		setName = inparam[3];
		binName = inparam[4];

		ClientPolicy policy = new ClientPolicy();
		policy.user = "";
		policy.password = "";
		policy.failIfNotConnected = true;

		client = new AerospikeClient(policy, host, port);

		writePolicy = new WritePolicy();

		if (dataType.equals("text-file"))
			runTextFile(args, argi);
		else if (dataType.equals("seq-int"))
			runSeqInt(args, argi);
		else
			throw new RuntimeException(String.format("unknown dataType \"%s\"",
																							 dataType));
	}

	public static void runTextFile(String[] args, int argi) throws Exception {

		while (argi < args.length) {
			String path = args[argi++];
			log.info("processing " + path + " ...");
			int nrecs = 0;
			BufferedReader br = new BufferedReader(new FileReader(path));
			for (String line; (line = br.readLine()) != null; ) {
				// The key is "path:linenum".
				String keystr = path + ':' + Long.toString(nrecs++);
				Key key = new Key(namespace, setName, keystr);
				Bin bin = new Bin(binName, line);
				client.put(writePolicy, key, bin);
			}
			log.info("inserted " + nrecs + " records");
		}
	}

	public static void runSeqInt(String[] args, int argi) throws Exception {

		int offset = Integer.parseInt(args[argi++]);
		int nrecs = Integer.parseInt(args[argi++]);

		String ndxname = binName + "ndx";
        
		IndexTask task =
			client.createIndex(null, namespace, setName,
												 ndxname, binName, IndexType.NUMERIC);

		task.waitTillComplete();
		log.info("created secondary index on " + binName);

		for (long ll = offset; ll < nrecs; ++ll) {

			String keystr = "key-" + ll;

			Key key = new Key(namespace, setName, keystr);
			Bin bin1 = new Bin(binName, ll);
			Bin bin2 = new Bin("bin2", "value2");

			client.put(writePolicy, key, bin1, bin2);
		}

		log.info("inserted " + nrecs + " records");
	}

	public static void main(String[] args) {

		try {
			log.info("starting");
			run(args);
			log.info("finished");
		} catch (Exception ex) {

			log.error(ex.getMessage());
			ex.printStackTrace();
		}
	}

}
