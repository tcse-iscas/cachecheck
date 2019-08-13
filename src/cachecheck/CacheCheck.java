package cachecheck;

import java.util.ArrayList;

public class CacheCheck {

	public static void main(String[] args) throws Exception {
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
		String traceFilePath = workspace+fileSeparator+appName+".trace";
		String jobFilePath = workspace+fileSeparator+appName+".job";
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
		util.detectBugs();
		System.out.println("Bug detection done!");
		System.out.println("Bugs:");
		util.printDetectionReport();
		System.out.println("Saving bug report to " + workspace+fileSeparator+appName+".report");
		util.saveReport(workspace+fileSeparator);
		System.out.println("Finished.");
	}

}
