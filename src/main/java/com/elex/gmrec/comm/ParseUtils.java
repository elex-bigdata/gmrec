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
