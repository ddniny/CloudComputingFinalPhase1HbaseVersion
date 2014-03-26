package HbaseDAO;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import HbaseBean.HbaseBean;

public class HbaseDAO {

	public HbaseDAO() throws IOException {
	}

	public void insert(HTable table, HbaseBean bean, String familyName, String colName) throws IOException{	
		String rowKey = bean.getUserId() + bean.getTweetTime();
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(familyName), Bytes.toBytes(colName), Bytes.toBytes(bean.getTweetId()));
		table.put(put);
		table.flushCommits();
	}
	
	public String get(HTable table, String rowKey, String familyName, String colName) throws IOException{
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addFamily(Bytes.toBytes(familyName));
		get.setMaxVersions(3);
		Result result = table.get(get);
		byte[] value = result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(colName));

		String valueStr = Bytes.toString(value);
		System.out.println("GET: " + valueStr);
		return valueStr;
	}

}
