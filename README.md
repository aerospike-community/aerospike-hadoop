Building
----------------------------------------------------------------

    ./gradlew :mapreduce:jar

    ./gradlew :examples:word_count:installApp


Running Sample Data Generator
----------------------------------------------------------------

    ./gradlew sampledata:run


Running Examples
----------------------------------------------------------------

Something like:

    export HADOOP_PREFIX=/usr/local/hadoop

    # Make a copy of /var/log/messages
    sudo cp /var/log/messages /tmp
    sudo chown $USER:$USER /tmp/messages
    chmod 644 /tmp/messages

    # Format HDFS
    rm -rf /tmp/hadoop-$USER/dfs/data
    $HADOOP_PREFIX/bin/hdfs namenode -format

    # Start HDFS
    $HADOOP_PREFIX/sbin/start-dfs.sh

    # Check for {Secondary,}NameNode and DataNode
    jps

     # Make some directories
    $HADOOP_PREFIX/bin/hdfs dfs -mkdir /tmp
    $HADOOP_PREFIX/bin/hdfs dfs -mkdir /tmp/$USER
    $HADOOP_PREFIX/bin/hdfs dfs -touchz /tmp/$USER/wordcount

    # Insert the sample data
    $HADOOP_PREFIX/bin/hadoop fs -put /tmp/messages /tmp/ksedgwic
    $HADOOP_PREFIX/bin/hadoop fs -ls /tmp/ksedgwic

    # Run the Hadoop job.
    cd ~/aerospike/aerospike-hadoop
    $HADOOP_PREFIX/bin/hdfs dfs -rm -r /tmp/output
    $HADOOP_PREFIX/bin/hadoop \
        jar \
        ./examples/word_count/build/libs/word_count.jar \
        /tmp/$USER/wordcount \
        /tmp/$USER \
        /tmp/output


    # Inspect the results.
    $HADOOP_PREFIX/bin/hadoop fs -ls /tmp/output
    rm -rf /tmp/output
    $HADOOP_PREFIX/bin/hadoop fs -copyToLocal /tmp/output /tmp
    less /tmp/output/part-r-00000

    # Stop HDFS
    $HADOOP_PREFIX/sbin/stop-dfs.sh
