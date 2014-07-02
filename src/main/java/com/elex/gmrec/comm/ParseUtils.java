package com.elex.gmrec.comm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;


public class ParseUtils {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	public static Map<String,Integer> readIdMapFile(FileSystem fs,Path src) throws IOException{
		Map<String,Integer> idMap = new HashMap<String,Integer>();
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(src))); 
		String line =reader.readLine();
        while(line != null){
        	String[] vList = line.split(",");
        	if(vList.length==2){
        		idMap.put(vList[1],Integer.parseInt(vList[0]));
        	}
        	
        	line = reader.readLine();
        }
        reader.close();
		return idMap;
		
	}
	
	
	public static Map<Integer,String> readIntStrIdMapFile(FileSystem fs,Path src) throws IOException{
		Map<Integer,String> idMap = new HashMap<Integer,String>();
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(src))); 
		String line =reader.readLine();
        while(line != null){
        	String[] vList = line.split(",");
        	if(vList.length==2){
        		idMap.put(Integer.parseInt(vList[0]),vList[1]);
        	}
        	
        	line = reader.readLine();
        }
        reader.close();
		return idMap;
		
	}
	
	public static void parseTextOutput(String input,String output,StrLineParseTool tool) throws Exception{
		 Configuration conf = new Configuration();
	     FileSystem fs = FileSystem.get(conf);
	     FileStatus[] files = fs.listStatus(new Path(input));
	     Path hdfs_src;
	     BufferedReader reader = null;
	     Path dist = new Path(output);
	     HdfsUtils.delFile(fs, dist.toString());
	     FSDataOutputStream out = fs.create(dist);
	     
	     for(FileStatus file:files){
	        	
	        	if(!file.isDirectory()){
	        		hdfs_src = file.getPath();
	        		if(file.getPath().getName().contains("part")){
	        	            reader = new BufferedReader(new InputStreamReader(fs.open(hdfs_src)));      	                    	            
	        	            String line =reader.readLine();
	        	            while(line != null){	        	         
	        	            	out.write(Bytes.toBytes(tool.parse(line)));
	        	            	line = reader.readLine();
	        	            }
	        	           IOUtils.closeStream(reader);
	        		}
	        	}
	        } 
	     out.close();
	}
}
