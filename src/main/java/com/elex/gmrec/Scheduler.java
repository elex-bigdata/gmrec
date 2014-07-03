package com.elex.gmrec;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Scheduler {
	
	private static final Logger log = LoggerFactory.getLogger(Scheduler.class);

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		AtomicInteger currentPhase = new AtomicInteger();
		String[] stageArgs = {otherArgs[0],otherArgs[1]};//运行阶段控制参数
		int success = 0;
		
		// stage 0
		if (shouldRunNextPhase(stageArgs, currentPhase)) {
			log.info("ETL START!!!");
			success = ETL();
			if (success != 0) {
				log.error("ETL ERROR!!!,SYSTEM EXIT!!!");
				System.exit(success);
			}
			log.info("ETL SUCCESS!!!");
		}
		
		// stage 1
		if (shouldRunNextPhase(stageArgs, currentPhase)) {
			log.info("CF START!!!");
			success = CF();
			if (success != 0) {
				log.error("CF ERROR!!!,SYSTEM EXIT!!!");
				System.exit(success);
			}
			log.info("CF SUCCESS!!!");
		}

		// stage 2
		if (shouldRunNextPhase(stageArgs, currentPhase)) {
			log.info("AR START!!!");
			success = AR();
			if (success != 0) {
				log.error("AR ERROR!!!,SYSTEM EXIT!!!");
				System.exit(success);
			}
			log.info("AR SUCCESS!!!");
		}

		// stage 3
		if (shouldRunNextPhase(stageArgs, currentPhase)) {
			log.info("LOAD START!!!");
			success = load();
			if (success != 0) {
				log.error("LOAD ERROR!!!,SYSTEM EXIT!!!");
				System.exit(success);
			}
			log.info("LOAD SUCCESS!!!");
		}
		
	}
	
	public static int ETL(){
		
		return 0;
	}
	
	public static int CF(){
		
		return 0;
	}
	
	public static int AR(){
		
		return 0;
	}
	
	public static int load(){
		
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
