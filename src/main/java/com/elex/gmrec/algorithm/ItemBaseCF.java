package com.elex.gmrec.algorithm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;

import com.elex.gmrec.comm.Constants;
import com.elex.gmrec.comm.ParseUtils;
import com.elex.gmrec.comm.PropertiesUtils;
import com.elex.gmrec.comm.StrLineParseTool;

public class ItemBaseCF implements StrLineParseTool {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		RunItemCf();
	}
	
	public static void RunItemCf() throws Exception{
		List<String> argList = new ArrayList<String>();
		argList.add("--input");
		argList.add(PropertiesUtils.getGmRecRootFolder()+Constants.CFINPUT);
		argList.add("--output");
		argList.add(PropertiesUtils.getGmRecRootFolder()+Constants.CFOUTPUT);
		argList.add("--numRecommendations");
		argList.add(PropertiesUtils.getCfNumOfRec());
		argList.add("--similarityClassname");
		argList.add(PropertiesUtils.getCfSimilarityClassname());
		
		String[] args = new String[argList.size()];
		argList.toArray(args);
		
		ToolRunner.run(new Configuration(), new RecommenderJob(), args);
	}
	
	public static void recParse() throws Exception{
		 String input = PropertiesUtils.getGmRecRootFolder()+Constants.CFOUTPUT;
		 String output = PropertiesUtils.getGmRecRootFolder()+Constants.CFRECPARSE;
		 ParseUtils.parseTextOutput(input, output, new ItemBaseCF());
	}
	
	
	public static void writeCfRecToRedis(){
		
	}

	@Override
	public String parse(String line) throws Exception {
		Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(conf);
		Path uidMappingFile = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.UIDMAPPINGFILE);
		Path gidMappingFile = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.GIDMAPPINGFILE);
		Map<Integer,String> uidMap = ParseUtils.readIntStrIdMapFile(fs,uidMappingFile);
	    Map<Integer,String> gidMap = ParseUtils.readIntStrIdMapFile(fs,gidMappingFile);
	    
	    String[] kv = line.toString().split("\\s");
	    String uid = uidMap.get(Integer.parseInt(kv[0].trim()));
		String itemStr = kv[1].trim().replace("[", "").replace("]", "");
		String[] itemArr = itemStr.split(",");
		StringBuffer sb = new StringBuffer(200);
		sb.append(uid+"\t");
		sb.append("[");
		for (int i = 0; i < itemArr.length; i++) {
			String[] item = itemArr[i].split(":");
			sb.append("{");
			String gid = gidMap.get(Integer.parseInt(item[0]));
			sb.append(gid+":"+item[1]);
			sb.append("}");
			if(i!=itemArr.length-1){
				sb.append(",");
			}
			
		}
		sb.append("]\r\n");
		return sb.toString();
	}

}
