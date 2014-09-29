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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

public class AerospikeConfigUtil {
	private static final Log log = LogFactory.getLog(AerospikeConfigUtil.class);

	public static final String INPUT_HOST = "aerospike.input.host";
	public static final String DEFAULT_INPUT_HOST = "localhost";
	public static final String INPUT_PORT = "aerospike.input.port";
	public static final int DEFAULT_INPUT_PORT = 3000;
	public static final String INPUT_NAMESPACE = "aerospike.input.namespace";
	public static final String INPUT_SETNAME = "aerospike.input.setname";
	public static final String INPUT_BINNAME = "aerospike.input.binname";
	public static final String INPUT_OPERATION = "aerospike.input.operation";
	public static final String DEFAULT_INPUT_OPERATION = "scan";
	public static final String INPUT_NUMRANGE_BEGIN = "aerospike.input.numrange.begin";
	public static final String INPUT_NUMRANGE_END = "aerospike.input.numrange.end";
	public static final long INVALID_LONG = 762492121482318889L;

	public static void setInputHost(Configuration conf, String host) {
		log.info("setting " + INPUT_HOST + " to " + host);
		conf.set(INPUT_HOST, host);
	}

	public static String getInputHost(Configuration conf) {
		String host = conf.get(INPUT_HOST, DEFAULT_INPUT_HOST);
		log.info("using " + INPUT_HOST + " = " + host);
		return host;
	}

	public static void setInputPort(Configuration conf, int port) {
		log.info("setting " + INPUT_PORT + " to " + port);
		conf.setInt(INPUT_PORT, port);
	}

	public static int getInputPort(Configuration conf) {
		int port = conf.getInt(INPUT_PORT, DEFAULT_INPUT_PORT);
		log.info("using " + INPUT_PORT + " = " + port);
		return port;
	}

	public static void setInputNamespace(Configuration conf, String namespace) {
		log.info("setting " + INPUT_NAMESPACE + " to " + namespace);
		conf.set(INPUT_NAMESPACE, namespace);
	}

	public static String getInputNamespace(Configuration conf) {
		String namespace = conf.get(INPUT_NAMESPACE);
		if (namespace == null)
			throw new UnsupportedOperationException("you must set the namespace");
		log.info("using " + INPUT_NAMESPACE + " = " + namespace);
		return namespace;
	}

	public static void setInputSetName(Configuration conf, String setname) {
		log.info("setting " + INPUT_SETNAME + " to " + setname);
		conf.set(INPUT_SETNAME, setname);
	}

	public static String getInputSetName(Configuration conf) {
		String setname = conf.get(INPUT_SETNAME);
		log.info("using " + INPUT_SETNAME + " = " + setname);
		return setname;
	}

	public static void setInputBinName(Configuration conf, String binname) {
		log.info("setting " + INPUT_BINNAME + " to " + binname);
		conf.set(INPUT_BINNAME, binname);
	}

	public static String getInputBinName(Configuration conf) {
		String binname = conf.get(INPUT_BINNAME);
		log.info("using " + INPUT_BINNAME + " = " + binname);
		return binname;
	}

	public static void setInputOperation(Configuration conf, String operation) {
		if (!operation.equals("scan") &&
				!operation.equals("numrange"))
			throw new UnsupportedOperationException("input operation must be 'scan' or 'numrange'");
		log.info("setting " + INPUT_OPERATION + " to " + operation);
		conf.set(INPUT_OPERATION, operation);
	}

	public static String getInputOperation(Configuration conf) {
		String operation = conf.get(INPUT_OPERATION, DEFAULT_INPUT_OPERATION);
		if (!operation.equals("scan") &&
				!operation.equals("numrange"))
			throw new UnsupportedOperationException("input operation must be 'scan' or 'numrange'");
		log.info("using " + INPUT_OPERATION + " = " + operation);
		return operation;
	}

	public static void setInputNumRangeBegin(Configuration conf, long begin) {
		log.info("setting " + INPUT_NUMRANGE_BEGIN + " to " + begin);
		conf.setLong(INPUT_NUMRANGE_BEGIN, begin);
	}

	public static long getInputNumRangeBegin(Configuration conf) {
		long begin = conf.getLong(INPUT_NUMRANGE_BEGIN, INVALID_LONG);
		if (begin == INVALID_LONG && getInputOperation(conf).equals("numrange"))
			throw new UnsupportedOperationException("missing numrange begin");
		log.info("using " + INPUT_NUMRANGE_BEGIN + " = " + begin);
		return begin;
	}

	public static void setInputNumRangeEnd(Configuration conf, long end) {
		log.info("setting " + INPUT_NUMRANGE_END + " to " + end);
		conf.setLong(INPUT_NUMRANGE_END, end);
	}

	public static long getInputNumRangeEnd(Configuration conf) {
		long end = conf.getLong(INPUT_NUMRANGE_END, INVALID_LONG);
		if (end == INVALID_LONG && getInputOperation(conf).equals("numrange"))
			throw new UnsupportedOperationException("missing numrange end");
		log.info("using " + INPUT_NUMRANGE_END + " = " + end);
		return end;
	}
}
