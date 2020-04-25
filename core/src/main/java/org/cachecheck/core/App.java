package org.cachecheck.core;

import java.util.ArrayList;
import java.util.Arrays;

public class App {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		ArrayList<String> argList = new ArrayList<String>(Arrays.asList(args));
		String usage = "An example: java -jar cachecheck-1.0-SNAPSHOT.jar $TraceDir $AppName [-d]\r\n"
				+ "$TraceDir is the directory that stores the trace files collected by running instrumented Spark. \r\n"
				+ "$AppName is the name of the application, which is also the file name of the trace file.\r\n"
				+ "[-d] Debug mode. If it is used, intermediate files for analysis, e.g., .job, .info, etc. will be maintained.";
		if(argList.contains("-h") || argList.contains("help")) {
			System.out.println("Welcome to use CacheCheck! \r\n"
					+ usage);
			System.exit(0);
		}
		boolean debug = false;
		if(argList.contains("-d"))
			debug = true;
		if(argList.size() < 2) {
			System.out.println("Wrong arguments! \r\n" + usage);
			System.exit(-1);
		}
		String workspace = args[0];
		int nameLength = args.length;
		String appName = args[1];
		for(int i = 2; i< nameLength; i++)
			appName += " " + args[i];
		String fileSeparator = "";
		String os = System.getProperty("os.name");
		if(os.toLowerCase().startsWith("win"))
			fileSeparator = "\\";
		else
			fileSeparator = "/";
		//String workspace = "E:\\Workspaces\\trace\\trace&report0815\\ml";
		//String appName = "GradientBoostedTreeClassifierExample";
		String filePath = workspace+fileSeparator+appName;
		String traceFilePath = filePath+".trace";
		String jobFilePath = filePath+".job";
		System.out.println("Begin to read job & trace file.");
		Util util = new Util(traceFilePath, jobFilePath, appName);
		System.out.println("Read job & trace file done.");
		System.out.println("Jobs:");
		ArrayList<DAG> jobs = util.jobs;
		for (DAG job : jobs) {
			for(Integer rdd : job.toArray()) {
				System.out.print(rdd+",");
			}
			System.out.println("");
		}
		
		System.out.println("Actual trace:");
		ArrayList<String> actSeq = util.getActualSequence();
		for (String string : actSeq) {
			System.out.print(string + ",");
		}
		System.out.println("");

		System.out.println("Begin to calculate RDDs which should be persisted.");
		System.out.println("RDDs should be persisted:");
		ArrayList<Integer> rdds = util.getShouldPersistRDDs();
		for (Integer integer : rdds) {
			System.out.print(integer+",");
		}
		System.out.println("");
		
		System.out.println("Begin to generate correct sequence.");
		System.out.println("Correct sequence:");
		ArrayList<String> rddSP = util.generateCorrectSequence();
		for (String string : rddSP) {
			System.out.print(string+",");
		}
		System.out.println("");
		
		System.out.println("Begin to detect bugs by comparing correct sequence with actual sequence.");
		ArrayList<Bug> report = util.detectBugs();
		System.out.println("Bug detection done!");
		System.out.println("Duplication...");
		util.initialPosInformation(filePath+".perpos", filePath+".info");
		report = util.deduplication(report);
		System.out.println("Bugs:");
		util.printDetectionReport(report);
		System.out.println("Saving bug report to " + filePath+".dreport");
		Util.saveReport(workspace+fileSeparator, util.generateDetailedReport(report), appName);
		System.out.println("Delete intermediate files...");
		if(!debug) util.deleteIntermediateFiles(workspace, appName);
		System.out.println("Finished.");
	}
}
