package com.elex.gmrec.algorithm;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.Pair;
import org.apache.mahout.fpm.pfpgrowth.convertors.string.TopKStringPatterns;

import com.elex.gmrec.comm.Constants;
import com.elex.gmrec.comm.HdfsUtils;
import com.elex.gmrec.comm.PropertiesUtils;
import com.elex.gmrec.etl.IDMapping;

public class AsocciationRule {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		runFPG();
		runARule();
	}
	
	public static int runFPG() throws Exception{
		List<String> argList = new ArrayList<String>();
		argList.add("-i");
		argList.add(PropertiesUtils.getGmRecRootFolder()+Constants.FPGINPUT);
		argList.add("-o");
		argList.add(PropertiesUtils.getGmRecRootFolder()+Constants.FIOUTPUT);
		argList.add("-s");
		argList.add(PropertiesUtils.getMinSupport());
		argList.add("-k");
		argList.add(PropertiesUtils.getNumberFrequentItem());
		argList.add("-method");
		argList.add("mapreduce");
		argList.add("-regex");
		argList.add("\"[\\ ]\"");		
		String[] args = new String[argList.size()];
		argList.toArray(args);		
		return ToolRunner.run(new Configuration(), new FrequentItem(), args);
	}
	
	public static int runARule() throws IOException{
		String[] gidIntStrMap = IDMapping.getGidIntStrMap();
		double confidence = PropertiesUtils.getConfidence();
		double theConfidence = 0D;
		DecimalFormat df = new DecimalFormat("#.###");
		String input = PropertiesUtils.getGmRecRootFolder()+Constants.FIOUTPUT+"/frequentpatterns";
		String output= PropertiesUtils.getGmRecRootFolder()+Constants.ARULEOUTPUT;
		
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] files = fs.listStatus(new Path(input));
        SequenceFile.Reader reader = null;
        
        Path dist = new Path(output);
	    HdfsUtils.delFile(fs, dist.toString());
	    FSDataOutputStream out = fs.create(dist);
	    
	    Text itemID = new Text(); 
	    TopKStringPatterns patterns = new TopKStringPatterns(); 
        Path hdfs_src =null;
        
        Long frequency = 0L;
        Iterator<Pair<List<String>, Long>>  ite;
        Pair<List<String>, Long> pattern;
        
        String gid;
       
        
        for(FileStatus file:files){
        	
        	if(!file.isDirectory()){
        		hdfs_src = file.getPath();
        		if(file.getPath().getName().contains("part")){
        			reader = new SequenceFile.Reader(conf, Reader.file(hdfs_src));        	                    	            
        	            while (reader.next(itemID, patterns)) { 
        	            	StringBuilder sb = new StringBuilder(100);
        	            	gid = gidIntStrMap[Integer.parseInt(itemID.toString())];
        	            	sb.append(gid).append("\t").append("[");
        	            	ite = patterns.iterator();
        	            	int i=0;
        	            	while(ite.hasNext()){
        	            		pattern = ite.next();
        	            		if(pattern.getFirst().size()==1){
        	            			frequency = pattern.getSecond();
        	            		}else if(pattern.getFirst().size()==2){
        	            			theConfidence = pattern.getSecond().doubleValue()/frequency.doubleValue();
        	            			if(theConfidence >= confidence){
        	            				sb.append("{");
        	            				sb.append("\"");
        	            				gid = gidIntStrMap[Integer.parseInt(pattern.getFirst().get(0))];
        	            				sb.append(gid);
        	            				sb.append("\"");
        	            				sb.append(":\"").append(df.format(theConfidence)+"-"+pattern.getSecond()+"\"");
        	            				sb.append("}");
        	            				sb.append(",");
        	            				i++;
        	            			}       	            			
        	            		}
        	            	}
        	            	if(i > 0){
            	            	out.write(Bytes.toBytes(new String(sb.toString().substring(0, sb.toString().length()-1)+"]\r\n")));	
        	            	}
        	            }        	            
        	           reader.close();
        	        
        		}
        	}
        }
        out.close();
		return 0;
	}

}
