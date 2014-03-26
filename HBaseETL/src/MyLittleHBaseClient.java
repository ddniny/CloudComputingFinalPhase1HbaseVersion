import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import etl.HbaseETL;


// Class that has nothing but a main.
// Does a Put, Get and a Scan against an hbase table.
public class MyLittleHBaseClient {
  public static void main(String[] args) throws IOException {
    // You need a configuration object to tell the client where to connect.
    // When you create a HBaseConfiguration, it reads in whatever you've set
    // into your hbase-site.xml and in hbase-default.xml, as long as these can
    // be found on the CLASSPATH    
//    Configuration conf = HBaseConfiguration.create();
//    conf.set("fs.hdfs.imple", "emr.hbase.fs.BlockableFileSystem");
//    conf.set("hbase.regionserver.handler.count", "100");
//    conf.set("hbase.zookeeper.quorum", "54.85.203.95");
//    conf.set("hbase.rootdir", "54.85.203.95:9000/hbase");
//    conf.set("hbase.zookeeper.property.clientPort","2181"); 
//    conf.set("hbase.cluster.distributed", "true");
//    conf.set("hbase.tmp.dir", "/mnt/var/lib/hbase/tmp-data");
    
//    final String TABLENAME = "people";
    HbaseETL etl = new HbaseETL();
    
  }
}