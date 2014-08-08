package com.elex.gmrec.algorithm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.ToolRunner;

import com.elex.gmrec.comm.Constants;
import com.elex.gmrec.comm.HdfsUtils;
import com.elex.gmrec.comm.ParseUtils;
import com.elex.gmrec.comm.PropertiesUtils;
import com.elex.gmrec.comm.StrLineParseTool;
import com.elex.gmrec.etl.IDMapping;

public class ItemBaseCF implements StrLineParseTool {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		RunItemCf();
		cfSimParse();
		recParse();
	}
	
	public static int RunItemCf() throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String cfOut = PropertiesUtils.getGmRecRootFolder()+Constants.CFOUTPUT;
		String cfTemp = PropertiesUtils.getGmRecRootFolder()+Constants.CFTEMP;
		HdfsUtils.delFile(fs, cfOut);
		HdfsUtils.delFile(fs, cfTemp);
		List<String> argList = new ArrayList<String>();
		argList.add("--input");
		argList.add(PropertiesUtils.getGmRecRootFolder()+Constants.CFINPUT);
		argList.add("--output");
		argList.add(cfOut);
		argList.add("--numRecommendations");
		argList.add(PropertiesUtils.getCfNumOfRec());
		argList.add("--similarityClassname");
		argList.add(PropertiesUtils.getCfSimilarityClassname());
		argList.add("--tempDir");
		argList.add(cfTemp);
		
		String[] args = new String[argList.size()];
		argList.toArray(args);
		
		return ToolRunner.run(new Configuration(), new org.apache.mahout.cf.taste.hadoop.item.RecommenderJob(), args);
	}
	
	public static int cfSimParse() throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String out = PropertiesUtils.getGmRecRootFolder()+Constants.CFSIMOUTPUT;
		HdfsUtils.delFile(fs, out);
		List<String> argList = new ArrayList<String>();
		argList.add(PropertiesUtils.getGmRecRootFolder()+Constants.CFTEMP);
		argList.add(out);
		argList.add(PropertiesUtils.getTopN());
		argList.add(PropertiesUtils.getThreshold());
		String[] args = new String[argList.size()];
		argList.toArray(args);
		return ToolRunner.run(new Configuration(), new SimilarityParse(),args);
	}
		
	
	public static int recParse() throws Exception{
		 String input = PropertiesUtils.getGmRecRootFolder()+Constants.CFOUTPUT;
		 String output = PropertiesUtils.getGmRecRootFolder()+Constants.CFRECPARSE;
		 ParseUtils.parseTextOutput(input, output, new ItemBaseCF());
		 return 0;
	}
	
		
	public static int writeCfRecToRedis(){
		return 0;
	}

	@Override
	public String parse(String line) throws Exception {		
		Map<Integer,String> uidMap = IDMapping.getUidIntStrMap();
	    Map<Integer,String> gidMap = IDMapping.getGidIntStrMap();
	    
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
			sb.append("\""+gid+"\":"+item[1]);
			sb.append("}");
			if(i!=itemArr.length-1){
				sb.append(",");
			}
			
		}
		sb.append("]\r\n");
		return sb.toString();
	}

}
