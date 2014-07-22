package com.elex.gmrec.etl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.elex.gmrec.comm.PropertiesUtils;

public class TagLoader {

	private static final Logger log = LoggerFactory.getLogger(TagLoader.class);
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		load();
	}
	
	public static int load() throws IOException{
		Configuration configuration = HBaseConfiguration.create();
		HTable gm = new HTable(configuration, "gm_gidlist");
		gm.setAutoFlush(false);
		File tagFile = new File(PropertiesUtils.getTagFile());
		BufferedReader in; 
		List<Put> tagPutList = new ArrayList<Put>();
		if(tagFile.exists()){
			in = new BufferedReader(new FileReader(tagFile));
			String line = in.readLine().trim();
			while (line != null) {
				String[] tag = line.split(",");
				if(tag.length==3){
					Put put = new Put(Bytes.add(Bytes.toBytes("m"), Bytes.toBytes(tag[0])));
					put.add(Bytes.toBytes("gm"), Bytes.toBytes("tagids"), Bytes.toBytes(tag[1]));
					put.add(Bytes.toBytes("gm"), Bytes.toBytes("tagnames"), Bytes.toBytes(tag[2]));
					tagPutList.add(put);
				}				
				line = in.readLine();
			}
			gm.put(tagPutList);
			in.close();
			gm.close();
		}else{
			log.error("tagFile is not exists!!!");
		}
		return 0;
	}

}
