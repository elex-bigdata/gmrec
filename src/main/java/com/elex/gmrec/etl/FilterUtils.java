package com.elex.gmrec.etl;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class FilterUtils {
	
	private static Set<String> miniGM;

	public static Set<String> getMiniGM() throws IOException {
		if(miniGM == null){
			miniGM = createMiniGmSet();
		}
		return miniGM;
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	private static Set<String> createMiniGmSet() throws IOException{
		
		Set<String> set = new HashSet<String>();
		Configuration configuration = HBaseConfiguration.create();
		HTable gm = new HTable(configuration, "gm_gidlist");
		gm.setAutoFlush(false);
		Scan s = new Scan();
		s.setCaching(500);
		s.addColumn(Bytes.toBytes("gm"), Bytes.toBytes("gt"));
		ResultScanner rs = gm.getScanner(s);
		for (Result r : rs) {
			if(!r.isEmpty()){
				if(r.containsColumn(Bytes.toBytes("gm"), Bytes.toBytes("gt"))){
					if(r.getColumnLatest(Bytes.toBytes("gm"), Bytes.toBytes("gt")).getValue() != null){
						if(Bytes.toString(r.getColumnLatest(Bytes.toBytes("gm"), Bytes.toBytes("gt")).getValue()).equals("m")){
							set.add(Bytes.toString(Bytes.tail(r.getRow(), r.getRow().length-1)));
						}
					}
					
				}
																														
			}
		}
		gm.close();
		return set;
		
	}
	
	
	public static void writeFilerFile(FileSystem fs, Set<String> set,Path dest) throws IOException{
		FSDataOutputStream out = fs.create(dest);
		Iterator<String> ite = set.iterator();
		Map<String,Integer> gidStrIntMap = IDMapping.getGidStrIntMap();
		while(ite.hasNext()){
			out.write(Bytes.toBytes(new String(gidStrIntMap.get(ite.next())+"\r\n")));
		}		
		out.close();		
	}

	
}
