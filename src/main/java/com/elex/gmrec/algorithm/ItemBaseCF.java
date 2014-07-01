package com.elex.gmrec.algorithm;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;

import com.elex.gmrec.comm.Constants;
import com.elex.gmrec.comm.PropertiesUtils;

public class ItemBaseCF {

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

}
