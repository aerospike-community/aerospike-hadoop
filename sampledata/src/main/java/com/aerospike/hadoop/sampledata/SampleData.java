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

    public static void run(String[] args) throws Exception {

		ClientPolicy policy = new ClientPolicy();
		policy.user = "";
		policy.password = "";
		policy.failIfNotConnected = true;

        String host = "localhost";
        int port = 3000;
		
		AerospikeClient client = new AerospikeClient(policy, host, port);

        String namespace = "test";
        String setName = "sample";

        String bin1name = "bin1";
        String ndxname = "bin1ndx";
        
        IndexTask task =
            client.createIndex(null, namespace, setName,
                               ndxname, bin1name, IndexType.NUMERIC);

        task.waitTillComplete();
        System.out.println("created secondary index on " + bin1name);

        WritePolicy writePolicy = new WritePolicy();

        int nrecs = 10 * 1000;

        for (int ii = 0; ii < nrecs; ++ii) {

            String keystr = "key-" + ii;

            Key key = new Key(namespace, setName, keystr);
            Bin bin1 = new Bin(bin1name, ii);
            Bin bin2 = new Bin("bin2", "value2");

            client.put(writePolicy, key, bin1, bin2);
        }

        System.out.println("inserted " + nrecs + " records");
    }

    public static void main(String[] args) {

        try {
            System.out.println("starting");
            run(args);
            System.out.println("finished");
        } catch (Exception ex) {

            System.out.println(ex.getMessage());
            ex.printStackTrace();
        }
    }

}
