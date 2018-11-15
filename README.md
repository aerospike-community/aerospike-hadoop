# Aerospike Hadoop Connector

This repository contains AerospikeInputFormat.java and
AerospikeOutputFormat.java, and several examples of processing using
Hadoop.

The system allows putting WorkerNodes on Aerospike servers. By
default, the AerospikeInputMapper will split according to the nodes on
the cluster, avoiding network traffic. The InputMapper also supports
using secondary indexes, thus pulling only a few of the records in the
Aerospike database.

Both new and old Hadoop interfaces are supported, and there are
examples for both.

In the case of using AerospikeOutputMapper, the Aerospike cluster is
likely to be outside the Hadoop worker nodes. This allows immediate
use of the Hadoop output in your application.

Check out the examples. The classic word count examples are included -
for both input and output. The "aggregate int example" uses a
secondary index to pull data from Aerospike, and runs the InputFormat
on the local node if available.

The most interesting example is likely the session rollup example.  In
this example, the session management state is output to Aerospike as
the sessions are found.

Install Hadoop
----------------------------------------------------------------
Examples below are tested with Aerospike Java Client (version: 4.2.2) and Hadoop (version: 2.7.2)

Hadoop installation guide [link](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html)

Then set up environment variable: 
----------------------------------------------------------------

    Hadoop Installation Directory:
    export HADOOP_PREFIX=/usr/local/hadoop

	Development Directory:
    export AEROSPIKE_HADOOP=~/aerospike/aerospike-hadoop


Build w/ Gradle
----------------------------------------------------------------

    cd ${AEROSPIKE_HADOOP}

    # Build the mapreduce input and output connectors.
    ./gradlew :mapreduce:jar

    # Build the example programs.
    ./gradlew :sampledata:installApp
    ./gradlew :examples:word_count_input:installApp
    ./gradlew :examples:aggregate_int_input:installApp
    ./gradlew :examples:word_count_output:installApp
    ./gradlew :examples:session_rollup:installApp
    ./gradlew :examples:generate_profiles:installApp
    ./gradlew :examples:external_join:installApp



Setup Target Input Text File
----------------------------------------------------------------

    # Make a copy of /var/log/messages
    sudo cp /var/log/messages /tmp/input
    sudo chown $USER:$USER /tmp/input
    chmod 644 /tmp/input


Start Aerospike
----------------------------------------------------------------

    sudo /etc/init.d/aerospike start


Setup Sample Data in Aerospike for Input Examples
----------------------------------------------------------------

    cd ${AEROSPIKE_HADOOP}/sampledata

    # Loads a text file for word_count_input demo.
    java -jar build/libs/sampledata.jar \
        localhost:3000:test:words:bin1 \
        text-file \
        /tmp/input

    # Generates sequential integers for aggregate_int_input demo.
    java -jar build/libs/sampledata.jar \
        localhost:3000:test:integers:bin1 seq-int 0 100000


Run Input Examples
----------------------------------------------------------------

    export HADOOP_PREFIX=/usr/local/hadoop

    cd ${AEROSPIKE_HADOOP}

    # Format HDFS
    rm -rf /tmp/hadoop-$USER/dfs/data
    $HADOOP_PREFIX/bin/hdfs namenode -format

    # Start HDFS
    $HADOOP_PREFIX/sbin/start-dfs.sh

    # Check for {Secondary,}NameNode and DataNode
    jps

     # Make some directories
    $HADOOP_PREFIX/bin/hdfs dfs -mkdir /tmp

    # Run the Hadoop job.
    cd ${AEROSPIKE_HADOOP}

    # Run the word_count_input example (Old Hadoop API)
    $HADOOP_PREFIX/bin/hdfs dfs -rm -r /tmp/output
    $HADOOP_PREFIX/bin/hadoop \
        jar \
        ./examples/word_count_input/build/libs/word_count_input.jar \
        -D aerospike.input.namespace=test \
        -D aerospike.input.setname=words \
        -D aerospike.input.operation=scan \
        /tmp/output

    # Jump to "Inspect the results" below ...

    # -- OR --

    # Run the aggregate_int_input range example (New Hadoop API)
    $HADOOP_PREFIX/bin/hdfs dfs -rm -r /tmp/output
    $HADOOP_PREFIX/bin/hadoop \
        jar \
        ./examples/aggregate_int_input/build/libs/aggregate_int_input.jar \
        -D aerospike.input.namespace=test \
        -D aerospike.input.setname=integers \
        -D aerospike.input.binnames=bin1 \
        -D aerospike.input.operation=scan \
        /tmp/output

    # Jump to "Inspect the results" below ...

    # -- OR --

    # Run the aggregate_int_input range example (New Hadoop API)
    $HADOOP_PREFIX/bin/hdfs dfs -rm -r /tmp/output
    $HADOOP_PREFIX/bin/hadoop \
        jar \
        ./examples/aggregate_int_input/build/libs/aggregate_int_input.jar \
        -D aerospike.input.namespace=test \
        -D aerospike.input.setname=integers \
        -D aerospike.input.binnames=bin1,bin2 \
        -D aerospike.input.operation=numrange \
        -D aerospike.input.numrange.bin=bin1 \
        -D aerospike.input.numrange.begin=100 \
        -D aerospike.input.numrange.end=200 \
        /tmp/output

    # Inspect the results.
    $HADOOP_PREFIX/bin/hadoop fs -ls /tmp/output
    rm -rf /tmp/output
    $HADOOP_PREFIX/bin/hadoop fs -copyToLocal /tmp/output /tmp
    less /tmp/output/part*00000


Setup Sample Data in HDFS for Output Examples
----------------------------------------------------------------

    export HADOOP_PREFIX=/usr/local/hadoop

    # Create a directory.
    $HADOOP_PREFIX/bin/hdfs dfs -mkdir /tmp

    # Load the test words into HDFS.
    $HADOOP_PREFIX/bin/hdfs dfs -rm /tmp/words
    $HADOOP_PREFIX/bin/hadoop fs -copyFromLocal /tmp/input /tmp/words

    # Load the World Cup log data into HDFS
    $HADOOP_PREFIX/bin/hdfs dfs -rm -r /worldcup
    $HADOOP_PREFIX/bin/hdfs dfs -mkdir /worldcup
    $HADOOP_PREFIX/bin/hadoop fs -copyFromLocal \
        data/worldcup\
        /worldcup/access.log

    # Create the secondary indexes in Aerospike.
    aql -c 'CREATE INDEX useridndx ON test.sessions (userid) NUMERIC'
    aql -c 'CREATE INDEX startndx ON test.sessions (start) NUMERIC'


Run Output Examples
----------------------------------------------------------------

    # Run the Hadoop job.
    cd ${AEROSPIKE_HADOOP}

    # Run the word_count_output example (Old Hadoop API)
    $HADOOP_PREFIX/bin/hadoop \
        jar \
        ./examples/word_count_output/build/libs/word_count_output.jar \
        -D aerospike.output.namespace=test \
        -D aerospike.output.setname=counts \
        /tmp/words

    # Inspect the results:
    aql -c 'SELECT * FROM test.counts'

    # -- OR --

    # Run the session_rollup example (Old Hadoop API, small dataset)
    $HADOOP_PREFIX/bin/hadoop \
        jar \
        ./examples/session_rollup/build/libs/session_rollup.jar \
        -D aerospike.output.namespace=test \
        -D aerospike.output.setname=sessions \
        -D mapred.reduce.tasks=30 \
        /worldcup/access.log

    # Inspect the results:
    aql -c 'SELECT * FROM test.sessions'

    # -- OR --

    # Run generate_profiles to build sample data for external_join.
    $HADOOP_PREFIX/bin/hadoop \
        jar \
        ./examples/generate_profiles/build/libs/generate_profiles.jar \
        -D aerospike.output.namespace=test \
        -D aerospike.output.setname=profiles \
        -D mapred.reduce.tasks=30 \
        /worldcup/access.log

    # Inspect the results:
    aql -c 'SELECT * FROM test.profiles'

    # -- AND --

    # Run the external_join example (Old Hadoop API, small dataset)
    $HADOOP_PREFIX/bin/hadoop \
        jar \
        ./examples/external_join/build/libs/external_join.jar \
        -D aerospike.input.namespace=test \
        -D aerospike.input.setname=profiles \
        -D aerospike.output.namespace=test \
        -D aerospike.output.setname=sessions2 \
        -D mapred.reduce.tasks=30 \
        /worldcup/access.log

    # Inspect the results:
    aql -c 'SELECT * FROM test.sessions2'
./gradlew :examples:word_count_input:installApp

Done with HDFS
----------------------------------------------------------------

    # Stop HDFS
    $HADOOP_PREFIX/sbin/stop-dfs.sh

