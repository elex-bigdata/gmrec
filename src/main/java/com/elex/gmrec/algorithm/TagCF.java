package com.elex.gmrec.algorithm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import com.elex.gmrec.comm.Constants;
import com.elex.gmrec.comm.HdfsUtils;
import com.elex.gmrec.comm.PropertiesUtils;
import com.elex.gmrec.comm.StrLineParseTool;
import com.elex.gmrec.etl.IDMapping;
import com.elex.gmrec.etl.PrepareInputForCF;

public class TagCF implements StrLineParseTool{

	

	//private Map<String,String> tagTopN;
	
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		prepare();
		RunItemCf();
	}
	
	/*
	 * 在运行协同过滤之前，需要对PrepareInputForTagCF的输入数据做处理，即将uid转为int型的索引，这里需要用到IDMapping输出的uid文件
	 */
	public static void prepare() throws Exception{
			
		String input = PropertiesUtils.getGmRecRootFolder()+Constants.TAGCFIN;
		String output = PropertiesUtils.getGmRecRootFolder()+Constants.TAGCFINFINAL;
		StrLineParseTool tool = new TagCF();
		PrepareInputForCF.prepareInput(input,output,tool);
		
	}
	
	public static int RunItemCf() throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String cfOut = PropertiesUtils.getGmRecRootFolder()+Constants.TAGCFOUTPUT;
		String cfTemp = PropertiesUtils.getGmRecRootFolder()+Constants.TAGCFTEMP;
		HdfsUtils.delFile(fs, cfOut);
		HdfsUtils.delFile(fs, cfTemp);
		List<String> argList = new ArrayList<String>();
		argList.add("--input");
		argList.add(PropertiesUtils.getGmRecRootFolder()+Constants.TAGCFINFINAL);
		argList.add("--output");
		argList.add(cfOut);
		argList.add("--numRecommendations");
		argList.add(PropertiesUtils.getTagCfNumOfRec());
		argList.add("--similarityClassname");
		argList.add(PropertiesUtils.getCfSimilarityClassname());
		argList.add("--tempDir");
		argList.add(cfTemp);
		
		String[] args = new String[argList.size()];
		argList.toArray(args);
		
		//使用的是改造后的itemCf算法
		return ToolRunner.run(new Configuration(), new org.apache.mahout.cf.taste.hadoop.item.RecommenderJob(), args);
	}
	
		
	
	/*public static int recParse() throws Exception{
		 String input = PropertiesUtils.getGmRecRootFolder()+Constants.TAGCFOUTPUT;
		 String output = PropertiesUtils.getGmRecRootFolder()+Constants.TAGCFRECPARSE;
		 ParseUtils.parseTextOutput(input, output, new TagCF());
		 return 0;
	}*/
	
	/*@Override
	public String parse(String line) throws Exception {		
		Map<Integer,String> uidMap = IDMapping.getUidIntStrMap();	
		if(tagTopN==null){
			tagTopN = getTagTopNMap();
		}
	    String[] kv = line.toString().split("\\s");
	    String uid = uidMap.get(Integer.parseInt(kv[0].trim()));
		String itemStr = kv[1].trim().replace("[", "").replace("]", "");
		String[] items = itemStr.split(",");
		StringBuffer sb = new StringBuffer(200);
		sb.append(uid+"\t");
		for(String item:items){
			sb.append(tagTopN.get(item.split(":")[0])).append(",");
		}					
		return sb.substring(0, sb.toString().length()-1)+"\r\n";
	}*/
	
	public static Map<String, List<String>> getTagTopNMap() throws IOException {
		int size = Integer.parseInt(PropertiesUtils.getItemRecNumber());
		Map<String, List<String>> tagTopN = new HashMap<String, List<String>>();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path output = new Path(PropertiesUtils.getGmRecRootFolder()+ Constants.TAGRANKOUT);
		FileStatus[] files = fs.listStatus(output);
		Path hdfs_src;
		for (FileStatus file : files) {
			if (!file.isDirectory()) {
				hdfs_src = file.getPath();
				if (file.getPath().getName().contains("part")) {
					BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(hdfs_src)));
					String line = reader.readLine();
					while (line != null) {
						String[] kv = line.split("\\s");
						List<String> list = new ArrayList<String>();
						String[] items = kv[1].split(",");
						for(String item:items){							
							list.add(item.split(":")[0]);
						}
						
						if(list.size()>size){
							tagTopN.put(kv[0], list.subList(0, size));
						}else if(list.size()<size && list.size()>0){
							tagTopN.put(kv[0], list);
						}
						
						line = reader.readLine();
					}
					reader.close();
				}

			}

		}
		return tagTopN;
	}
	
	@Override
	public String parse(String line) throws Exception {
		Map<String,Integer> uidMap = IDMapping.getUidStrIntMap();
		
		String[] vList = line.split(",");
		
    	if(vList.length==3){
        	return new String(Integer.toString(uidMap.get(vList[0]))+","+vList[1]+","+vList[2]+"\r\n");
    	}
		return null;
	}
	
}
