Install Hadoop
----------------------------------------------------------------

    export HADOOPVER=2.5.1

    cd /usr/local/dist
    wget http://mirrors.ibiblio.org/apache/hadoop/common/stable/hadoop-${HADOOPVER}.tar.gz
    cd /usr/local
    tar xvfz /usr/local/dist/hadoop-${HADOOPVER}.tar.gz
    ln -s hadoop-${HADOOPVER} hadoop
    
    # Add default FS in /usr/local/hadoop/etc/hadoop/core-site.xml

    export HADOOP_PREFIX=/usr/local/hadoop


Building
----------------------------------------------------------------

    cd ~/aerospike/aerospike-hadoop

    # Build the mapreduce input and output connectors.
    ./gradlew :mapreduce:jar

    # Build the example programs.
    ./gradlew :examples:word_count_input:installApp
    ./gradlew :examples:int_sum_input:installApp
    ./gradlew :examples:word_count_output:installApp
    ./gradlew :examples:session_rollup:installApp


Setup Target Input Text File
----------------------------------------------------------------

    # Make a copy of /var/log/messages
    sudo cp /var/log/messages /tmp/input
    sudo chown $USER:$USER /tmp/input
    chmod 644 /tmp/input


Start Aerospike
----------------------------------------------------------------

    cd ~/aerospike/aerospike-server
    make start


Setup Sample Data
----------------------------------------------------------------

    cd ~/aerospike/aerospike-hadoop

    # Loads a text file for word_count_input demo.
    ./gradlew sampledata:run \
        -PappArgs="['localhost:3000:test:words:bin1', \
                    'text-file', \
                    '/tmp/input']"

    # Generates sequential integers for int_sum_input demo.
    ./gradlew sampledata:run \
        -PappArgs="['localhost:3000:test:integers:bin1', \
                    'seq-int', \
                    '10000']"

    # Load log files for the session_rollup demo.
    ./gradlew sampledata:run \
        -PappArgs="['localhost:3000:test:logrecs:bin1', \
                'text-file', \
                '/home/ksedgwic/aerospike/doc/data/WorldCup/wc_day52_1.log', \
                '/home/ksedgwic/aerospike/doc/data/WorldCup/wc_day52_2.log']"


Running Input Examples
----------------------------------------------------------------

    export HADOOP_PREFIX=/usr/local/hadoop

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
    cd ~/aerospike/aerospike-hadoop

    # Run the word_count_input example (Old Hadoop API)
    $HADOOP_PREFIX/bin/hdfs dfs -rm -r /tmp/output
    $HADOOP_PREFIX/bin/hadoop \
        jar \
        ./examples/word_count_input/build/libs/word_count_input.jar \
        -D aerospike.input.namespace=test \
        -D aerospike.input.setname=words \
        -D aerospike.input.binname=bin1 \
        -D aerospike.input.operation=scan \
        /tmp/output

    # -- OR --

    # Run the int_sum_input example (New Hadoop API)
    $HADOOP_PREFIX/bin/hdfs dfs -rm -r /tmp/output
    $HADOOP_PREFIX/bin/hadoop \
        jar \
        ./examples/int_sum_input/build/libs/int_sum_input.jar \
        -D aerospike.input.namespace=test \
        -D aerospike.input.setname=integers \
        -D aerospike.input.binname=bin1 \
        -D aerospike.input.operation=scan \
        /tmp/output

    # -- OR --

    # Run the int_sum_input range example (New Hadoop API)
    $HADOOP_PREFIX/bin/hdfs dfs -rm -r /tmp/output
    $HADOOP_PREFIX/bin/hadoop \
        jar \
        ./examples/int_sum_input/build/libs/int_sum_input.jar \
        -D aerospike.input.namespace=test \
        -D aerospike.input.setname=integers \
        -D aerospike.input.binname=bin1 \
        -D aerospike.input.operation=numrange \
        -D aerospike.input.numrange.begin=100 \
        -D aerospike.input.numrange.end=200 \
        /tmp/output

    # -- OR --

    # Run the session_rollup example (Old Hadoop API)
    $HADOOP_PREFIX/bin/hdfs dfs -rm -r /tmp/output
    $HADOOP_PREFIX/bin/hadoop \
        jar \
        ./examples/session_rollup/build/libs/session_rollup.jar \
        -D aerospike.input.namespace=test \
        -D aerospike.input.setname=logrecs \
        -D aerospike.input.binname=bin1 \
        -D aerospike.input.operation=scan \
        /tmp/wc_day52_1.log \
        /tmp/wc_day52_2.log \
        /tmp/output

    # Inspect the results.
    $HADOOP_PREFIX/bin/hadoop fs -ls /tmp/output
    rm -rf /tmp/output
    $HADOOP_PREFIX/bin/hadoop fs -copyToLocal /tmp/output /tmp
    less /tmp/output/part*00000

    # Stop HDFS
    $HADOOP_PREFIX/sbin/stop-dfs.sh


Running Output Examples
----------------------------------------------------------------

    export HADOOP_PREFIX=/usr/local/hadoop

    # Format HDFS
    rm -rf /tmp/hadoop-$USER/dfs/data
    $HADOOP_PREFIX/bin/hdfs namenode -format

    # Start HDFS
    $HADOOP_PREFIX/sbin/start-dfs.sh

    # Check for {Secondary,}NameNode and DataNode
    jps

     # Load the test words into HDFS.
    $HADOOP_PREFIX/bin/hdfs dfs -mkdir /tmp
    $HADOOP_PREFIX/bin/hdfs dfs -rm /tmp/words
    $HADOOP_PREFIX/bin/hadoop fs -copyFromLocal /tmp/input /tmp/words

    # Run the Hadoop job.
    cd ~/aerospike/aerospike-hadoop

    # Run the word_count_output example (Old Hadoop API)
    $HADOOP_PREFIX/bin/hdfs dfs -rm -r /tmp/output
    $HADOOP_PREFIX/bin/hadoop \
        jar \
        ./examples/word_count_output/build/libs/word_count_output.jar \
        -D aerospike.output.namespace=test \
        -D aerospike.output.setname=counts \
        -D aerospike.output.binname=value \
        -D aerospike.output.keyname=key \
        /tmp/words

    # Inspect the results:
    ~/aerospike/aerospike-tools/asql/target/Linux-x86_64/bin/aql \
        -c 'SELECT * FROM test.counts'

    # Stop HDFS
    $HADOOP_PREFIX/sbin/stop-dfs.sh


----------------------------------------------------------------

     # Load the test words into HDFS.
    $HADOOP_PREFIX/bin/hdfs dfs -mkdir /tmp
    $HADOOP_PREFIX/bin/hdfs dfs -rm /tmp/wc_day52_1.log
    $HADOOP_PREFIX/bin/hdfs dfs -rm /tmp/wc_day52_2.log
    $HADOOP_PREFIX/bin/hadoop fs -copyFromLocal ~ksedgwic/aerospike/doc/data/WorldCup/wc_day52_1.log /tmp/wc_day52_1.log
    $HADOOP_PREFIX/bin/hadoop fs -copyFromLocal ~ksedgwic/aerospike/doc/data/WorldCup/wc_day52_2.log /tmp/wc_day52_2.log

    # Run the session_rollup example (Old Hadoop API)
    $HADOOP_PREFIX/bin/hdfs dfs -rm -r /tmp/output
    $HADOOP_PREFIX/bin/hadoop \
        jar \
        ./examples/session_rollup/build/libs/session_rollup.jar \
        /tmp/wc_day52_1.log \
        /tmp/wc_day52_2.log \
        /tmp/output

    # Run the session_rollup example (Old Hadoop API)
    $HADOOP_PREFIX/bin/hdfs dfs -rm -r /tmp/output
    $HADOOP_PREFIX/bin/hadoop \
        jar \
        ./examples/session_rollup/build/libs/session_rollup.jar \
        -D aerospike.input.namespace=test \
        -D aerospike.input.setname=logrecs \
        -D aerospike.input.binname=bin1 \
        -D aerospike.input.operation=scan \
        /tmp/output
