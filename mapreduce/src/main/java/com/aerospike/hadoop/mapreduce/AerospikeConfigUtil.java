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

    // ---------------- INPUT ----------------

    public static final int DEFAULT_INPUT_PORT = 3000;
    public static final long INVALID_LONG = 762492121482318889L;

    // ---------------- OUTPUT ----------------

    public static final int DEFAULT_OUTPUT_PORT = 3000;

    // ---------------- INPUT ----------------

    public static void setInputHost(Configuration conf, String host) {
        log.info("setting " + AerospikeConfigEnum.INPUT_HOST.value + " to " + host);
        conf.set(AerospikeConfigEnum.INPUT_HOST.value, host);
    }

    public static String getInputHost(Configuration conf) {
        String host = conf.get(AerospikeConfigEnum.INPUT_HOST.value, AerospikeConfigEnum.DEFAULT_INPUT_HOST.value);
        log.info("using " + AerospikeConfigEnum.INPUT_HOST.value + " = " + host);
        return host;
    }

    public static void setInputPort(Configuration conf, int port) {
        log.info("setting " + AerospikeConfigEnum.INPUT_PORT.value + " to " + port);
        conf.setInt(AerospikeConfigEnum.INPUT_PORT.value, port);
    }

    public static int getInputPort(Configuration conf) {
        int port = conf.getInt(AerospikeConfigEnum.INPUT_PORT.value, DEFAULT_INPUT_PORT);
        log.info("using " + AerospikeConfigEnum.INPUT_PORT.value + " = " + port);
        return port;
    }

    public static void setInputNamespace(Configuration conf, String namespace) {
        log.info("setting " + AerospikeConfigEnum.INPUT_NAMESPACE.value + " to " + namespace);
        conf.set(AerospikeConfigEnum.INPUT_NAMESPACE.value, namespace);
    }

    public static String getInputNamespace(Configuration conf) {
        String namespace = conf.get(AerospikeConfigEnum.INPUT_NAMESPACE.value);
        if (namespace == null)
            throw new UnsupportedOperationException
                ("you must set the input namespace");
        log.info("using " + AerospikeConfigEnum.INPUT_NAMESPACE.value + " = " + namespace);
        return namespace;
    }

    public static void setInputSetName(Configuration conf, String setname) {
        log.info("setting " + AerospikeConfigEnum.INPUT_SETNAME.value + " to " + setname);
        conf.set(AerospikeConfigEnum.INPUT_SETNAME.value, setname);
    }

    public static String getInputSetName(Configuration conf) {
        String setname = conf.get(AerospikeConfigEnum.INPUT_SETNAME.value);
        log.info("using " + AerospikeConfigEnum.INPUT_SETNAME.value + " = " + setname);
        return setname;
    }

    public static void setInputBinNames(Configuration conf, String bins) {
        log.info("setting " + AerospikeConfigEnum.INPUT_BINNAMES.value + " to " + bins);
        conf.set(AerospikeConfigEnum.INPUT_BINNAMES.value, bins);
    }

    public static String[] getInputBinNames(Configuration conf) {
        String bins = conf.get(AerospikeConfigEnum.INPUT_BINNAMES.value);
        log.info("using " + AerospikeConfigEnum.INPUT_BINNAMES.value + " = " + bins);
        if (bins == null || bins.equals(""))
            return null;
        else
            return bins.split(",");
    }

    public static void setInputOperation(Configuration conf, String operation) {
        if (!operation.equals("scan") &&
                !operation.equals("numrange"))
            throw new UnsupportedOperationException
                ("input operation must be 'scan' or 'numrange'");
        log.info("setting " + AerospikeConfigEnum.INPUT_OPERATION.value + " to " + operation);
        conf.set(AerospikeConfigEnum.INPUT_OPERATION.value, operation);
    }

    public static String getInputOperation(Configuration conf) {
        String operation = conf.get(AerospikeConfigEnum.INPUT_OPERATION.value, AerospikeConfigEnum.DEFAULT_INPUT_OPERATION.value);
        if (!operation.equals("scan") &&
                !operation.equals("numrange"))
            throw new UnsupportedOperationException
                ("input operation must be 'scan' or 'numrange'");
        log.info("using " + AerospikeConfigEnum.INPUT_OPERATION.value + " = " + operation);
        return operation;
    }

    public static void setInputNumRangeBin(Configuration conf, String binname) {
        log.info("setting " + AerospikeConfigEnum.INPUT_NUMRANGE_BIN.value + " to " + binname);
        conf.set(AerospikeConfigEnum.INPUT_NUMRANGE_BIN.value, binname);
    }

    public static String getInputNumRangeBin(Configuration conf) {
        String binname = conf.get(AerospikeConfigEnum.INPUT_NUMRANGE_BIN.value);
        log.info("using " + AerospikeConfigEnum.INPUT_NUMRANGE_BIN.value + " = " + binname);
        return binname;
    }

    public static void setInputNumRangeBegin(Configuration conf, long begin) {
        log.info("setting " + AerospikeConfigEnum.INPUT_NUMRANGE_BEGIN.value + " to " + begin);
        conf.setLong(AerospikeConfigEnum.INPUT_NUMRANGE_BEGIN.value, begin);
    }

    public static long getInputNumRangeBegin(Configuration conf) {
        long begin = conf.getLong(AerospikeConfigEnum.INPUT_NUMRANGE_BEGIN.value, INVALID_LONG);
        if (begin == INVALID_LONG && getInputOperation(conf).equals("numrange"))
            throw new UnsupportedOperationException
                ("missing input numrange begin");
        log.info("using " + AerospikeConfigEnum.INPUT_NUMRANGE_BEGIN.value + " = " + begin);
        return begin;
    }

    public static void setInputNumRangeEnd(Configuration conf, long end) {
        log.info("setting " + AerospikeConfigEnum.INPUT_NUMRANGE_END.value + " to " + end);
        conf.setLong(AerospikeConfigEnum.INPUT_NUMRANGE_END.value, end);
    }

    public static long getInputNumRangeEnd(Configuration conf) {
        long end = conf.getLong(AerospikeConfigEnum.INPUT_NUMRANGE_END.value, INVALID_LONG);
        if (end == INVALID_LONG && getInputOperation(conf).equals("numrange"))
            throw new UnsupportedOperationException
                ("missing input numrange end");
        log.info("using " + AerospikeConfigEnum.INPUT_NUMRANGE_END.value + " = " + end);
        return end;
    }

    // ---------------- OUTPUT ----------------

    public static void setOutputHost(Configuration conf, String host) {
        log.info("setting " + AerospikeConfigEnum.OUTPUT_HOST.value + " to " + host);
        conf.set(AerospikeConfigEnum.OUTPUT_HOST.value, host);
    }

    public static String getOutputHost(Configuration conf) {
        String host = conf.get(AerospikeConfigEnum.OUTPUT_HOST.value, AerospikeConfigEnum.DEFAULT_OUTPUT_HOST.value);
        log.info("using " + AerospikeConfigEnum.OUTPUT_HOST.value + " = " + host);
        return host;
    }

    public static void setOutputPort(Configuration conf, int port) {
        log.info("setting " + AerospikeConfigEnum.OUTPUT_PORT.value + " to " + port);
        conf.setInt(AerospikeConfigEnum.OUTPUT_PORT.value, port);
    }

    public static int getOutputPort(Configuration conf) {
        int port = conf.getInt(AerospikeConfigEnum.OUTPUT_PORT.value, DEFAULT_OUTPUT_PORT);
        log.info("using " + AerospikeConfigEnum.OUTPUT_PORT.value + " = " + port);
        return port;
    }

    public static void setOutputNamespace(Configuration conf, String namespace) {
        log.info("setting " + AerospikeConfigEnum.OUTPUT_NAMESPACE.value + " to " + namespace);
        conf.set(AerospikeConfigEnum.OUTPUT_NAMESPACE.value, namespace);
    }

    public static String getOutputNamespace(Configuration conf) {
        String namespace = conf.get(AerospikeConfigEnum.OUTPUT_NAMESPACE.value);
        if (namespace == null)
            throw new UnsupportedOperationException
                ("you must set the output namespace");
        log.info("using " + AerospikeConfigEnum.OUTPUT_NAMESPACE.value + " = " + namespace);
        return namespace;
    }

    public static void setOutputSetName(Configuration conf, String setname) {
        log.info("setting " + AerospikeConfigEnum.OUTPUT_SETNAME.value + " to " + setname);
        conf.set(AerospikeConfigEnum.OUTPUT_SETNAME.value, setname);
    }

    public static String getOutputSetName(Configuration conf) {
        String setname = conf.get(AerospikeConfigEnum.OUTPUT_SETNAME.value);
        log.info("using " + AerospikeConfigEnum.OUTPUT_SETNAME.value + " = " + setname);
        return setname;
    }

    public static void setOutputBinName(Configuration conf, String binname) {
        log.info("setting " + AerospikeConfigEnum.OUTPUT_BINNAME.value + " to " + binname);
        conf.set(AerospikeConfigEnum.OUTPUT_BINNAME.value, binname);
    }

    public static String getOutputBinName(Configuration conf) {
        String binname = conf.get(AerospikeConfigEnum.OUTPUT_BINNAME.value);
        log.info("using " + AerospikeConfigEnum.OUTPUT_BINNAME.value + " = " + binname);
        return binname;
    }

    public static void setOutputKeyName(Configuration conf, String keyname) {
        log.info("setting " + AerospikeConfigEnum.OUTPUT_KEYNAME.value + " to " + keyname);
        conf.set(AerospikeConfigEnum.OUTPUT_KEYNAME.value, keyname);
    }

    public static String getOutputKeyName(Configuration conf) {
        String keyname = conf.get(AerospikeConfigEnum.OUTPUT_KEYNAME.value);
        log.info("using " + AerospikeConfigEnum.OUTPUT_KEYNAME.value + " = " + keyname);
        return keyname;
    }

    // ---------------- COMMON ----------------

    public static org.apache.hadoop.mapred.JobConf asJobConf(Configuration cfg) {
        return cfg instanceof org.apache.hadoop.mapred.JobConf
            ? (org.apache.hadoop.mapred.JobConf) cfg
            : new org.apache.hadoop.mapred.JobConf(cfg);
    }
}

// Local Variables:
// mode: java
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: nil
// End:
// vim: softtabstop=4:shiftwidth=4:expandtab
