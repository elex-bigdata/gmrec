package com.elex.gmrec.analyze;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class ActionDataReader {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {	
		gameAction(args[0]);
	}

	public static void gameAction(String action) throws IOException{		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		BufferedWriter out = new BufferedWriter(new FileWriter("game.csv"));
		Configuration conf = HBaseConfiguration.create();		
		HTable uc= new HTable(conf,"gm_user_action");
		long now =System.currentTimeMillis();
        long before = now - Long.valueOf(30L*24L*60L*60L*1000L); 
        
		Scan s = new Scan();
		s.setStartRow(Bytes.add(Bytes.toBytes(action), Bytes.toBytes(before)));
		s.setStopRow(Bytes.add(Bytes.toBytes(action), Bytes.toBytes(now)));
		s.setCaching(1000);
		s.addColumn(Bytes.toBytes("ua"), Bytes.toBytes("gid"));
		s.addColumn(Bytes.toBytes("ua"), Bytes.toBytes("gt"));
		s.addColumn(Bytes.toBytes("ua"), Bytes.toBytes("cl"));
		ResultScanner rs = uc.getScanner(s);
		String gid="";
		String uid="";
		String lang="";
		int id = 1;
		String[] ugid;
		Date dayTime = null;
		String gmType = null;
		String actionType = null;
				
		for(Result r:rs){
			
			if (!r.isEmpty()) {
				ugid = Bytes.toString(Bytes.tail(r.getRow(), r.getRow().length-10)).split("\u0001");
				if(ugid.length==2){
					uid = ugid[1];
				}
				
				dayTime = new Date(Bytes.toLong(Bytes.tail(Bytes.head(r.getRow(), 10), 8)));
				actionType = Bytes.toString(Bytes.head(r.getRow(), 2));
				
				for (KeyValue kv : r.raw()) {					
					if ("ua".equals(Bytes.toString(kv.getFamily()))&& "gid".equals(Bytes.toString(kv.getQualifier()))) {
						gid = Bytes.toString(kv.getValue());
					}
					if ("ua".equals(Bytes.toString(kv.getFamily()))&& "gt".equals(Bytes.toString(kv.getQualifier()))) {
						gmType = Bytes.toString(kv.getValue());
					}
					if ("ua".equals(Bytes.toString(kv.getFamily()))&& "cl".equals(Bytes.toString(kv.getQualifier()))) {
						lang = Bytes.toString(kv.getValue());
					}
				}
			}
			
			gmType=gmType==null?"mini":gmType;
			
			out.write(id+","+actionType+","+uid+","+gid+","+gmType+","+lang+","+sdf.format(dayTime)+"\r\n");
			id++;
			
		}
		uc.close();
		out.close();
	}
}
