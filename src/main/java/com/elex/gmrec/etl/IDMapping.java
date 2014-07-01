package com.elex.gmrec.etl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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

public class IDMapping {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		createIdMappingFile();
	}

	public static void createIdMappingFile() throws IOException{
		String uri = PropertiesUtils.getRatingFolder()+Constants.MERGEFOLDER;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] files = fs.listStatus(new Path(uri));
        Path hdfs_src;
        BufferedReader reader = null;
        Set<String> uidSet = new HashSet<String>();
        Set<String> gidSet = new HashSet<String>();		
		
        for(FileStatus file:files){
        	
        	if(!file.isDirectory()){
        		hdfs_src = file.getPath();
        		if(file.getPath().getName().contains("part")){
        			try {
        	            reader = new BufferedReader(new InputStreamReader(fs.open(hdfs_src)));      	                    	            
        	            String line =reader.readLine();
        	            while(line != null){
        	            	String[] vList = line.split(",");
        	            	uidSet.add(vList[0]);
        	            	gidSet.add(vList[1]);
        	            	line = reader.readLine();
        	            }
        	           reader.close();
        	        } finally {
        	            IOUtils.closeStream(reader);
        	        }
        			
        		}
        	}
        } 
        
        Path uidMappingFile = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.UIDMAPPINGFILE);
        HdfsUtils.delFile(fs, uidMappingFile.toString());
        writeSetToFile(fs,uidSet,uidMappingFile);
        
        Path gidMappingFile = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.GIDMAPPINGFILE);
        HdfsUtils.delFile(fs, gidMappingFile.toString());
        writeSetToFile(fs,gidSet,gidMappingFile);
        

	}
	
	public static void writeSetToFile(FileSystem fs, Set<String> set,Path dest) throws IOException{
		FSDataOutputStream out = fs.create(dest);
		Iterator<String> ite = set.iterator();
		int i = 1;
		while(ite.hasNext()){
			out.write(Bytes.toBytes(new String(i+","+ite.next()+"\r\n")));
			i++;
		}		
		out.close();		
	}
}
