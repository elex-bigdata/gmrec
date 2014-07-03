package com.elex.gmrec;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.elex.gmrec.algorithm.AsocciationRule;
import com.elex.gmrec.algorithm.ItemBaseCF;
import com.elex.gmrec.etl.IDMapping;
import com.elex.gmrec.etl.PrepareInputForCF;
import com.elex.gmrec.etl.PrepareInputForFPG;
import com.elex.gmrec.etl.Rating;
import com.elex.gmrec.etl.RatingMerge;


public class Scheduler {
	
	private static final Logger log = LoggerFactory.getLogger(Scheduler.class);

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		AtomicInteger currentPhase = new AtomicInteger();
		String[] stageArgs = {otherArgs[0],otherArgs[1]};//运行阶段控制参数
		int success = 0;
		
		// stage 0
		if (shouldRunNextPhase(stageArgs, currentPhase)) {
			log.info("ETL START!!!");
			success = ETL(args);
			if (success != 0) {
				log.error("ETL ERROR!!!,SYSTEM EXIT!!!");
				System.exit(success);
			}
			log.info("ETL SUCCESS!!!");
		}
		
		// stage 1
		if (shouldRunNextPhase(stageArgs, currentPhase)) {
			log.info("CF START!!!");
			success = CF(args);
			if (success != 0) {
				log.error("CF ERROR!!!,SYSTEM EXIT!!!");
				System.exit(success);
			}
			log.info("CF SUCCESS!!!");
		}

		// stage 2
		if (shouldRunNextPhase(stageArgs, currentPhase)) {
			log.info("AR START!!!");
			success = AR(args);
			if (success != 0) {
				log.error("AR ERROR!!!,SYSTEM EXIT!!!");
				System.exit(success);
			}
			log.info("AR SUCCESS!!!");
		}

		// stage 3
		if (shouldRunNextPhase(stageArgs, currentPhase)) {
			log.info("LOAD START!!!");
			success = load(args);
			if (success != 0) {
				log.error("LOAD ERROR!!!,SYSTEM EXIT!!!");
				System.exit(success);
			}
			log.info("LOAD SUCCESS!!!");
		}
		
	}
	
	public static int ETL(String[] args) throws Exception{
		log.info("RATING START!!!");
		ToolRunner.run(new Configuration(), new Rating(), args);
		log.info("RATING SUCCESS!!!");
		
		log.info("RATING MERGE START!!!");
		ToolRunner.run(new Configuration(), new RatingMerge(), args);
		log.info("RATING MERGE SUCCESS!!!");
		
		log.info("CREATE IDMAPPING FILE START!!!");
		IDMapping.createIdMappingFile();
		log.info("CREATE IDMAPPING FILE SUCCESS!!!");
		
		log.info("PREPARE INPUT FOR CF START!!!");
		PrepareInputForCF.prepareInput();
		log.info("PREPARE INPUT FOR CF SUCCESS!!!");
		
		log.info("PREPARE INPUT FOR FPG START!!!");
		ToolRunner.run(new Configuration(), new PrepareInputForFPG(), args);
		log.info("PREPARE INPUT FOR FPG SUCCESS!!!");
		
		return 0;
	}
	
	public static int CF(String[] args) throws Exception{
		
		log.info("ITEM BASED CF START!!!");
		ItemBaseCF.RunItemCf();
		log.info("ITEM BASED CF SUCCESS!!!");
		
		log.info("CF SIMILARITIES PARSE START!!!");
		ItemBaseCF.cfSimParse();
		log.info("CF SIMILARITIES PARSE SUCCESS!!!");
		
		log.info("CF REC PARSE START!!!");
		ItemBaseCF.recParse();
		log.info("CF REC PARSE SUCCESS!!!");
		
		return 0;
	}
	
	public static int AR(String[] args) throws Exception{
		
		log.info("FREQUENT ITEM MINING START!!!");
		AsocciationRule.runFPG();
		log.info("FREQUENT ITEM MINING SUCCESS!!!");
		
		log.info("ASSOCIATION RULE MINING START!!!");
		AsocciationRule.runARule();
		log.info("ASSOCIATION RULE MINING SUCCESS!!!");
		return 0;
	}
	
	public static int load(String[] args){
		
		return 0;
	}
	
	protected static boolean shouldRunNextPhase(String[] args, AtomicInteger currentPhase) {
	    int phase = currentPhase.getAndIncrement();
	    String startPhase = args[0];
	    String endPhase = args[1];
	    boolean phaseSkipped = (startPhase != null && phase < Integer.parseInt(startPhase))
	        || (endPhase != null && phase > Integer.parseInt(endPhase));
	    if (phaseSkipped) {
	      log.info("Skipping phase {}", phase);
	    }
	    return !phaseSkipped;
	  }

}
