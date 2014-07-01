package com.elex.gmrec.etl;

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

import com.elex.gmrec.comm.Constants;
import com.elex.gmrec.comm.HdfsUtils;
import com.elex.gmrec.comm.PropertiesUtils;

public class PrepareInputFroCF {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		prepareInput();
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
	
	public static void prepareInput() throws IOException{
		 Path uidMappingFile = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.UIDMAPPINGFILE);
		 Path gidMappingFile = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.GIDMAPPINGFILE);
		 Configuration conf = new Configuration();
	     FileSystem fs = FileSystem.get(conf);
	     String uri = PropertiesUtils.getRatingFolder()+Constants.MERGEFOLDER;
	     Map<String,Integer> uidMap = readIdMapFile(fs,uidMappingFile);
	     Map<String,Integer> gidMap = readIdMapFile(fs,gidMappingFile);
	     FileStatus[] files = fs.listStatus(new Path(uri));
	     Path hdfs_src;
	     BufferedReader reader = null;
	     Path dist = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.CFINPUT);
	     HdfsUtils.delFile(fs, dist.toString());
	     FSDataOutputStream out = fs.create(dist);
	     
	     for(FileStatus file:files){
	        	
	        	if(!file.isDirectory()){
	        		hdfs_src = file.getPath();
	        		if(file.getPath().getName().contains("part")){
	        	            reader = new BufferedReader(new InputStreamReader(fs.open(hdfs_src)));      	                    	            
	        	            String line =reader.readLine();
	        	            while(line != null){
	        	            	String[] vList = line.split(",");
	        	            	if(vList.length==3){
		        	            	out.write(Bytes.toBytes(new String(Integer.toString(uidMap.get(vList[0]))
		        	            			+","+Integer.toString(gidMap.get(vList[1]))+","+vList[2])));
	        	            	}
	        	            	line = reader.readLine();
	        	            }
	        	           IOUtils.closeStream(reader);
	        		}
	        	}
	        } 
	     out.close();
	}

}
