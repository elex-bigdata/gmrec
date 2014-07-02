package com.elex.gmrec.comm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;


public class ParseUtils {
			
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		readSeqFileToLocal(args[0],args[1]);

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
	
	
	public static void readSeqFileToLocal(String hdfsFile, String localFile)
			throws IOException {
		Configuration conf = new Configuration();
		Path path = new Path(hdfsFile);
		SequenceFile.Reader reader = null;
		File dstFile = new File(localFile);
		BufferedWriter out = new BufferedWriter(new FileWriter(dstFile));
		try {
			reader = new SequenceFile.Reader(conf, Reader.file(path));
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			while (reader.next(key, value)) {
				out.write(key + "," + value + "\r\n");
			}
			out.close();
		} finally {
			IOUtils.closeStream(reader);
		}
	}
}
