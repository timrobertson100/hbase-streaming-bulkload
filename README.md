### Example of bulk loading HBase from a Spark stream

This code demonstrates streaming bulk load into HBase `1.2.x` using Spark `1.6.x`.
A Netcat process is used to simulate a stream of data.

To run this example:
   1. Start an HBase server in standalone mode by  [downloading version 1.2.5](http://mirrors.dotsrc.org/apache/hbase/1.2.5/hbase-1.2.5-bin.tar.gz) and then issuing `./bin/start-hbase.sh`.  Verify it works by checking the HBase Master UI console is visible on [http://localhost:16010/master-status](http://localhost:16010/master-status).
   
   2. Create the table in HBase:
```
$ ./bin/hbase shell
hbase(main):034:0> create 'streamingtest', {NAME => 'c', VERSIONS => 1} 
```

   3.  The example uses a socket to provide the stream.  On a linux machine, start Netcat using `$ nc -lk 7001` before running the example.  On the Netcat terminal you can then write sample messages to be read from the stream.   
  
   4. The project `pom` contains a profile named `ide` which allows you to run the example in Intellij by simply enabling the `ide` profile and then right-click `Bulkload` and `Run Bulkload` (this assumes an HBase cluster is running locally).

       
Each entry writen in the Netcat console should result in a new row in HBase.  The rows are written in bulk every 10 seconds. 


  

