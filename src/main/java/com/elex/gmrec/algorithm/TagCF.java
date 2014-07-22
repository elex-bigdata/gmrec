package com.elex.gmrec.algorithm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ToolRunner;

import com.elex.gmrec.comm.Constants;
import com.elex.gmrec.comm.HdfsUtils;
import com.elex.gmrec.comm.ParseUtils;
import com.elex.gmrec.comm.PropertiesUtils;
import com.elex.gmrec.comm.StrLineParseTool;
import com.elex.gmrec.etl.IDMapping;

public class TagCF implements StrLineParseTool{

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		RunItemCf();
		recParse();
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
		argList.add(PropertiesUtils.getGmRecRootFolder()+Constants.TAGCFIN);
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
		
		return ToolRunner.run(new Configuration(), new RecommenderJob(), args);
	}
	
		
	
	public static int recParse() throws Exception{
		 String input = PropertiesUtils.getGmRecRootFolder()+Constants.TAGCFOUTPUT;
		 String output = PropertiesUtils.getGmRecRootFolder()+Constants.TAGCFRECPARSE;
		 ParseUtils.parseTextOutput(input, output, new TagCF());
		 return 0;
	}
	
	@Override
	public String parse(String line) throws Exception {		
		Map<Integer,String> uidMap = IDMapping.getUidIntStrMap();	
		Map<String,String> tagTopN = getTagTopNMap();
	    String[] kv = line.toString().split("\\s");
	    String uid = uidMap.get(Integer.parseInt(kv[0].trim()));
		String itemStr = kv[1].trim().replace("[", "").replace("]", "");
		String[] items = itemStr.split(",");
		StringBuffer sb = new StringBuffer(200);
		sb.append(uid+"\\t");
		for(String item:items){
			sb.append(tagTopN.get(item.split(":")[0])).append(",");
		}					
		return sb.substring(0, sb.toString().length()-1);
	}
	
	public static Map<String,String> getTagTopNMap() throws IOException{
		 Map<String,String> tagTopN = new HashMap<String,String>();
		 Configuration conf=new Configuration();
		 Path output = new Path(PropertiesUtils.getGmRecRootFolder()+Constants.TAGRANKOUT);
		 SequenceFile.Reader reader=null;
		 reader=new SequenceFile.Reader(conf,Reader.file(output));
		 Writable key=(Writable)ReflectionUtils.newInstance(reader.getKeyClass(),conf);
		 Writable value=(Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
		 while(reader.next(key,value)){	
			 tagTopN.put(key.toString(), value.toString());
		 }
		 reader.close();
		 return tagTopN;
	}
	
}
