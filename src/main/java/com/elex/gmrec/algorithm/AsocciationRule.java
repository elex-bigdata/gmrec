package com.elex.gmrec.algorithm;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import com.elex.gmrec.comm.Constants;
import com.elex.gmrec.comm.PropertiesUtils;

public class AsocciationRule {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		runFPG();
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

}
