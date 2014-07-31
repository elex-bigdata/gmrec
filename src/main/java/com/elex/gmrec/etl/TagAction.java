package com.elex.gmrec.etl;

import java.io.Serializable;

public class TagAction implements Serializable {
	
	/**
	 * 用户对tag的行为记录
	 */
	private static final long serialVersionUID = 1704174531512342797L;
	

	private int times = 0;//用户对该tag的行为次数
	private double rate = 0;//用户对该tag的打分之后
	
	public int getTimes() {
		return times;
	}
	public void setTimes(int t) {
		this.times = this.times+t;
	}
	public double getRate() {
		return rate;
	}
	public void setRate(double r) {
		this.rate = this.rate+r;
	}
	
	public double getRealRate(){
		return rate/times;
	}
	

}
