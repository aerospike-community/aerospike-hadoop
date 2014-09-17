Building
----------------------------------------------------------------

    ./gradlew :mapreduce:jar

    ./gradlew :examples:word_count:installApp


Setup Target Input Text File
----------------------------------------------------------------

    # Make a copy of /var/log/messages
    sudo cp /var/log/messages /tmp
    sudo chown $USER:$USER /tmp/input
    chmod 644 /tmp/input


Setup Sample Data
----------------------------------------------------------------

    ./gradlew sampledata:run \
       -PappArgs="['localhost:3000:test:sample', 'insertTextFile', '/tmp/input']"

Running Examples
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
    $HADOOP_PREFIX/bin/hdfs dfs -rm -r /tmp/output
    $HADOOP_PREFIX/bin/hadoop \
        jar \
        ./examples/word_count/build/libs/word_count.jar \
        localhost:3000:test:sample \
        /tmp/output


    # Inspect the results.
    $HADOOP_PREFIX/bin/hadoop fs -ls /tmp/output
    rm -rf /tmp/output
    $HADOOP_PREFIX/bin/hadoop fs -copyToLocal /tmp/output /tmp
    less /tmp/output/part-r-00000

    # Stop HDFS
    $HADOOP_PREFIX/sbin/stop-dfs.sh
