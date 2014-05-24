package wells.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTest {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		@SuppressWarnings("deprecation")
		HBaseConfiguration conf = new HBaseConfiguration();
		conf.addResource("/usr/local/hbase-0.98.2/conf/hbase-site.xml");
		conf.set("hbase.master", "localhost:60000");
		CreateTable(conf);
		SeedData(conf);
	}

	public static void CreateTable(HBaseConfiguration conf) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor tableDescriptor = new HTableDescriptor(
				TableName.valueOf("people"));
		tableDescriptor.addFamily(new HColumnDescriptor("personal"));
		tableDescriptor.addFamily(new HColumnDescriptor("contactinfo"));
		tableDescriptor.addFamily(new HColumnDescriptor("creditcard"));
		admin.createTable(tableDescriptor);
	}

	public static void SeedData(HBaseConfiguration conf) throws IOException {
		HTable table = new HTable(conf, "people");
		Put put = new Put(Bytes.toBytes("doe-john-m-12345"));
		put.add(Bytes.toBytes("personal"), Bytes.toBytes("givenName"),
				Bytes.toBytes("John"));
		put.add(Bytes.toBytes("personal"), Bytes.toBytes("mi"),
				Bytes.toBytes("M"));
		put.add(Bytes.toBytes("personal"), Bytes.toBytes("surame"),
				Bytes.toBytes("Doe"));
		put.add(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"),
				Bytes.toBytes("john.m.doe@gmail.com"));
		table.put(put);
		table.flushCommits();
		table.close();
	}

}
