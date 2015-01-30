package com.hadoop.demo.expense;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class CalendarUtil {

	// 获取某年某月 的天数  
	// 如，2月 有28天  or 29天
	public static int getDays(String y, String m) {
		try{
        	SimpleDateFormat simpleDate = new SimpleDateFormat("yyyy/MM");
        	Calendar rightNow = Calendar.getInstance();
    	    rightNow.setTime(simpleDate.parse(y+"/"+m));
        	return rightNow.getActualMaximum(Calendar.DAY_OF_MONTH);
    	}catch(Exception e){
    	    e.printStackTrace();
    	}
    	return 0;
	}
}
