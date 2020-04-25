package org.cachecheck.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;

import org.cachecheck.core.BugReport;
import org.cachecheck.core.Pattern;
import org.cachecheck.core.Util;

public class Deduplicator {
	public static void main( String[] args ) throws Exception
    {
		ArrayList<String> argList = new ArrayList<String>(Arrays.asList(args));
		String usage = "An example: java -jar tools-deduplicator.jar  $ReportDir \r\n"
				+ "$ReportDir is the directory where .dreport files locate.\r\n";
		if(argList.contains("-h") || argList.contains("help")) {
			System.out.println("Welcome to use Deduplicator! \r\n"
					+ "Deduplicator will summarize all bug reports and generate a list of unique bugs. \r\n"
					+ usage);
			System.exit(0);
		} else if(argList.size() != 1) {
			System.out.println("Wrong arguments! \r\n" + usage);
			System.exit(-1);
		}
    	String workspace = args[0];
        File dir = null;
        if(workspace != null) {
        	dir = new File(workspace);
        }
        if(dir.exists() && dir.isDirectory()) {
        	File[] reports = dir.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					// TODO Auto-generated method stub
					return name.endsWith(".dreport");
				}
			});
        	ArrayList<BugReport> bugs = new ArrayList<BugReport>();
        	for (File report : reports) {
        		FileInputStream fisReport = new FileInputStream(report);
    			BufferedReader brReport = new BufferedReader(new InputStreamReader(fisReport));
    			String bugString;
    			brReport.readLine();// The first line is the header.
    			while ((bugString = brReport.readLine()) != null) {
    				BugReport bug = new BugReport();
    				String[] bugInfos = bugString.split("\t ");
    				bug.rdd = bugInfos[1];
    				bug.pattern = Pattern.getPattern(bugInfos[2]);
    				bug.p_upinformation[0] = bugInfos[3];
    				bug.p_upinformation[1] = bugInfos[4];
    				bug.firstUseAction = bugInfos[5];
    				bug.lastUseAction = bugInfos[6];
    				boolean newBug = true;
    				for(BugReport anotheBug : bugs) {
    					if(bug.isSame(anotheBug)) {
    						newBug = false;
    						break;
    					}
    				}
    				if(newBug)
    					bugs.add(bug);
    			}
    			brReport.close();
    			fisReport.close();
			}
        	Util.saveReport(workspace, bugs, "summarize");
        	System.out.println("The bug summarize has been stored in " + workspace + "summarize.dreport!");
        }
    }
}
