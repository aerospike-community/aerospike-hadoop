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
    ./gradlew :examples:word_count:installApp
    ./gradlew :examples:int_sum:installApp


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

    # Loads a text file for word_count demo.
    ./gradlew sampledata:run \
        -PappArgs="['localhost:3000:test:words:bin1', \
                    'text-file', \
                    '/tmp/input']"

    # Generates sequential integers for int_sum demo.
    ./gradlew sampledata:run \
        -PappArgs="['localhost:3000:test:integers:bin1', \
                    'seq-int', \
                    '10000']"


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

    # Run the word_count example (Old Hadoop API)
    $HADOOP_PREFIX/bin/hdfs dfs -rm -r /tmp/output
    $HADOOP_PREFIX/bin/hadoop \
        jar \
        ./examples/word_count/build/libs/word_count.jar \
        scan:localhost:3000:test:words:bin1 \
        /tmp/output

    # -- OR --

    # Run the int_sum example (New Hadoop API)
    $HADOOP_PREFIX/bin/hdfs dfs -rm -r /tmp/output
    $HADOOP_PREFIX/bin/hadoop \
        jar \
        ./examples/int_sum/build/libs/int_sum.jar \
        scan:localhost:3000:test:integers:bin1 \
        /tmp/output

    # -- OR --

    # Run the int_sum range example (New Hadoop API)
    $HADOOP_PREFIX/bin/hdfs dfs -rm -r /tmp/output
    $HADOOP_PREFIX/bin/hadoop \
        jar \
        ./examples/int_sum/build/libs/int_sum.jar \
        numrange:localhost:3000:test:integers:bin1:100:200 \
        /tmp/output

    # Inspect the results.
    $HADOOP_PREFIX/bin/hadoop fs -ls /tmp/output
    rm -rf /tmp/output
    $HADOOP_PREFIX/bin/hadoop fs -copyToLocal /tmp/output /tmp
    less /tmp/output/part*00000

    # Stop HDFS
    $HADOOP_PREFIX/sbin/stop-dfs.sh
