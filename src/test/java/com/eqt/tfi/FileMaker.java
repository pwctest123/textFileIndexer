package com.eqt.tfi;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import org.joda.time.DateTime;

/**
 * Use this to randomly generate files to play with. :)
 * @author gman
 *
 */
public class FileMaker {

	/**
	 * @param args
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws InterruptedException, IOException {
		if (args.length <= 2) {
			System.out.println("USAGE: FileMaker tmpDir destDir [pauseMSBetweenFiles]");
			System.exit(1);
		}
		
		int pause = 0;
		String tmpDir = args[0];
		if(!tmpDir.endsWith("/"))
			tmpDir += "/";
		
		File td = new File(tmpDir);
		if(!td.exists())
			td.mkdirs();
		
		String destDir = args[1];
		if(!destDir.endsWith("/"))
			destDir += "/";
		
		td = new File(destDir);
		if(!td.exists())
			td.mkdirs();

		if(args.length == 3)
			pause = Integer.parseInt(args[2]);

		Random r = new Random();
		while(true) {
			
			DateTime dt = new DateTime();
			String fileName = dt.getYear() + "" + dt.getMonthOfYear() + "" + dt.getDayOfMonth() + "-" +
					dt.getHourOfDay() + "" + dt.getMinuteOfHour() + "" +
					dt.getSecondOfDay() + "" + dt.getMillisOfSecond();
			
			File f = new File(tmpDir + fileName);
			
			BufferedWriter w = new BufferedWriter(new FileWriter(f));
			for(int lines = 0; lines < r.nextInt(10000);lines++) {
				for(int words = 0; words < r.nextInt(10);words++)
					w.write(r.nextInt()+" ");
				w.write("\n");
			}
			w.close();
			File to = new File(destDir + f.getName());
			f.renameTo(to);
			System.out.println("wrote: " + to.toString());
			
			if(pause != 0)
				Thread.sleep(pause);
		}
		
	}

}
