package com.aerospike.hadoop.mapreduce;

public enum AerospikeConfigEnum {
	
    // ---------------- OUTPUT ----------------

    INPUT_HOST("aerospike.input.host"),
    DEFAULT_INPUT_HOST("localhost"),
    INPUT_PORT("aerospike.input.port"),
    INPUT_NAMESPACE("aerospike.input.namespace"),
    INPUT_SETNAME("aerospike.input.setname"),
    INPUT_BINNAMES("aerospike.input.binnames"),
    DEFAULT_INPUT_BINNAMES(""),
    INPUT_OPERATION("aerospike.input.operation"),
    DEFAULT_INPUT_OPERATION("scan"),
    INPUT_SCAN_PERCENT("aerospike.input.scan.percent"),
    INPUT_NUMRANGE_BIN("aerospike.input.numrange.bin"),
    INPUT_NUMRANGE_BEGIN("aerospike.input.numrange.begin"),
    INPUT_NUMRANGE_END("aerospike.input.numrange.end"),

    // ---------------- OUTPUT ----------------

    OUTPUT_HOST("aerospike.output.host"),
    DEFAULT_OUTPUT_HOST("localhost"),
    OUTPUT_PORT("aerospike.output.port"),
    OUTPUT_NAMESPACE("aerospike.output.namespace"),
    OUTPUT_SETNAME("aerospike.output.setname"),
    OUTPUT_BINNAME("aerospike.output.binname"),
    OUTPUT_KEYNAME("aerospike.output.keyname");

    public final String value;
    
    private AerospikeConfigEnum(String v){
    	value = v;
    }
}
