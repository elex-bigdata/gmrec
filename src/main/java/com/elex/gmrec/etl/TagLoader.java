package com.elex.gmrec.etl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.elex.gmrec.comm.Language;
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
	
	/*
	 * 加载gmtag.csv文件到hbase的gm_gidlist表,gmtag.csv的格式为gid，language，tagids，tagnames，例如
	 * 123go_motorcycle_racing,en,34:118:148,Car Games:Motorcycle Games:Racing Games
	 */
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
				if(tag.length==4){
					Put put = new Put(Bytes.add(Bytes.toBytes("m"), Bytes.toBytes(tag[0])));
					put.add(Bytes.toBytes("gm"), Bytes.toBytes(tag[1]), Bytes.toBytes(tag[2]));
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
	
	/*
	 *读取hbase的gm_gidlist表，获取gid对应语言的对应tag，返回值是map，key为gid，
	 *value为语种和tagid映射表，该映射表也是一个map，映射表map的key为语种标识，value为tagid列表
	 */
	public static Map<String,Map<String,String>> getGidTagMap() throws IOException{
		HTable gm;
		Configuration configuration;
		Map<String,Map<String,String>> result = new HashMap<String,Map<String,String>>();
		Map<String,String> gameTagMap;
		String gid,language,tags;
		configuration = HBaseConfiguration.create();
		gm = new HTable(configuration, "gm_gidlist");
		gm.setAutoFlush(false);
		Scan s = new Scan();
		s.setCaching(500);
		Language[] ls = Language.values();
		for(Language l : ls){
			s.addColumn(Bytes.toBytes("gm"), Bytes.toBytes(l.name()));
		}		
		ResultScanner rs = gm.getScanner(s);
		
		for (Result r : rs) {
			if (!r.isEmpty()) {
				gameTagMap = new HashMap<String,String>();
				gid = Bytes.toString(Bytes.tail(r.getRow(),r.getRow().length - 1));
				for (KeyValue kv : r.raw()) {
					language = Bytes.toString(kv.getQualifier());
					tags = Bytes.toString(kv.getValue());
					gameTagMap.put(language,tags);
					
				}
				result.put(gid,gameTagMap);
			}
		}
		gm.close();
		//System.out.println(result.get("war_of_guns").get("es"));
		return result;
	}
	
	public static String getTag(Map<String,String> gameTagMap){
		Language[] lang = Language.values();
		String tags;
		for(Language l : lang){
			tags = gameTagMap.get(l.name());
			if(tags != null){
				return tags;
			}
		}
		
		return com.elex.gmrec.comm.Constants.DEFAULTTAG;
	}
	

}
