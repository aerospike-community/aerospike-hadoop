Install Hadoop
----------------------------------------------------------------

    export HADOOPVER=2.5.1

    cd /usr/local/dist
    wget http://mirrors.ibiblio.org/apache/hadoop/common/stable/hadoop-${HADOOPVER}.tar.gz
    cd /usr/local
    tar xvfz /usr/local/dist/hadoop-${HADOOPVER}.tar.gz
    ln -s hadoop-${HADOOPVER} hadoop
    
    export HADOOP_PREFIX=/usr/local/hadoop


Building
----------------------------------------------------------------

    ./gradlew :mapreduce:jar

    ./gradlew :examples:word_count:installApp


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

    ./gradlew sampledata:run \
        -PappArgs="['localhost:3000:test:sample:bin1', \
                    'text-file', \
                    '/tmp/input']"

    ./gradlew sampledata:run \
        -PappArgs="['localhost:3000:test:sample', 'seq-int', '10000']"


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
        localhost:3000:test:sample:bin1 \
        /tmp/output

    # Inspect the results.
    $HADOOP_PREFIX/bin/hadoop fs -ls /tmp/output
    rm -rf /tmp/output
    $HADOOP_PREFIX/bin/hadoop fs -copyToLocal /tmp/output /tmp
    less /tmp/output/part-00000

    # Stop HDFS
    $HADOOP_PREFIX/sbin/stop-dfs.sh
