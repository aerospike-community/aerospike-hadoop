Building
----------------------------------------------------------------

    ./gradlew jar

    ./gradlew :examples:word_count:installApp


Running Sample Data Generator
----------------------------------------------------------------

    ./gradlew sampledata:run


Running Examples
----------------------------------------------------------------

Something like:

    export HADOOP_PREFIX=/usr/local/hadoop
    export HADOOP=$HADOOP_PREFIX/bin/hadoop
    export HDFS=$HADOOP_PREFIX/bin/hdfs

    # Make a copy of /var/log/messages
    sudo cp /var/log/messages /tmp
    sudo chown ksedgwic:ksedgwic /tmp/messages
    chmod 644 /tmp/messages

    # Format HDFS
    bin/hdfs namenode -format

    # Start HDFS
    sbin/start-dfs.sh

    # Check for {Secondary,}NameNode and DataNode
    jps

     # Make some directories
    $HDFS dfs -mkdir /tmp
    $HDFS dfs -mkdir /tmp/$USER
    $HDFS dfs -touchz /tmp/$USER/wordcount
    $HDFS dfs -rmdir /tmp/output

    $HADOOP jar ./examples/word_count/build/libs/word_count.jar \
         /tmp/$USER/wordcount /tmp/$USER /tmp/output

    $HADOOP fs -ls /tmp/$USER

    $HADOOP fs -copyToLocal /tmp/output /tmp

