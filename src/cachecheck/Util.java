package cachecheck;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

public class Util {
	ArrayList<DAG> jobs;
	private ArrayList<Integer> rddshouldpersit;
	private ArrayList<String> actualSequence;
	private ArrayList<String> correctSequence;
	private Map<Integer, String> detectionReport;
	private String appName = "default";
	
	public Util(String traceFilePath, String jobFilePath) {
		jobs = new ArrayList<DAG>();
		rddshouldpersit = new ArrayList<Integer>();
		actualSequence = new ArrayList<String>();
		correctSequence = new ArrayList<String>();
		detectionReport = new HashMap<Integer, String>();
		try {
		appName = traceFilePath.substring(traceFilePath.lastIndexOf("\\"), traceFilePath.lastIndexOf("."));}
		catch (Exception e) {
			System.out.println("Please format the name of trace file!");
		}
		// Read jobfile
		File jobFile = new File(jobFilePath);
		try {
			FileInputStream fisJob = new FileInputStream(jobFile);
			BufferedReader brJob = new BufferedReader(new InputStreamReader(fisJob));
			String jobString;
			while((jobString = brJob.readLine()) != null) {
				if(!jobString.equals(""))
					jobs.add(jobAsDAG(jobString));
			}
			brJob.close();
			fisJob.close();
		} catch (IOException e) {
			System.out.println("[ERROR] .job file error!");
		}
		// generate job dag objects
		for(int i = 0; i < jobs.size(); i++) {
			for(int j = i+1; j < jobs.size(); j++) {
				DAG job2 = jobs.get(j);
				ArrayList<Integer> branchPoints = jobs.get(i).getLargestBranchPoint(job2);
				rddshouldpersit.addAll(branchPoints);
			}
		}
		HashSet<Integer> set = new HashSet<Integer>(rddshouldpersit);
		rddshouldpersit = new ArrayList<Integer>(set);
		
		//read tracefile
		File traceFile = new File(traceFilePath);
		try {
			FileInputStream fisTrace = new FileInputStream(traceFile);
			BufferedReader brTrace = new BufferedReader(new InputStreamReader(fisTrace));
			String traceString;
			while((traceString = brTrace.readLine())!=null) {
				actualSequence.add(traceString);
			}
			brTrace.close();
			fisTrace.close();
		} catch (IOException e) {
			System.out.println("[ERROR] .trace file error!");
		}
	}
	
	private DAG jobAsDAG(String job) {
		String[] edges = job.split(",");
		String youngest = edges[0].split("-")[1];
		Vertex youngestV = new Vertex(Integer.parseInt(youngest));
		DAG dag = new DAG(youngestV);
		for(int i=1; i<edges.length;i++) {
			String[] rddPairs = edges[i].split("-");
			dag.addEdge(Integer.parseInt(rddPairs[1]), Integer.parseInt(rddPairs[0]));
		}
		return dag;
	}
	
	public ArrayList<Integer> getShouldPersistRDDs() {
		return rddshouldpersit;
	}
	
	public ArrayList<String> getActualSequence() {
		return actualSequence;
	}
	
	public ArrayList<String> generateCorrectSequence() throws Exception {
		ArrayList<String> result = new ArrayList<String>();
		ArrayList<Integer> rddFinishPersist = new ArrayList<Integer>();
		// Key for rddid, value for id of job which uses the rdd.
		HashMap<Integer, Integer> useCondition = new HashMap<Integer, Integer>();
		int jobNum = jobs.size();
		// generate the correct persist positions
		for(int i=0; i<jobNum; i++) {
			DAG job = jobs.get(i);
			for(Integer rddsp: rddshouldpersit) {
				if(!rddFinishPersist.contains(rddsp) && job.hasVertex(rddsp)) {
					result.add("persist "+ rddsp);
					rddFinishPersist.add(rddsp);
					useCondition.put(rddsp, i);
				}
			}
			result.add("job "+i);
		}
		// generate the correct unpersist positions
		for(Integer rddsp: rddshouldpersit) {
			int firstUse = useCondition.get(rddsp);
			int lastUse = firstUse;
			boolean takeover = false;
			for(int i=firstUse+1; i<jobNum; i++) {
				DAG job = jobs.get(i);
				if(job.hasVertex(rddsp)) {
					if(!takeover)
						lastUse = i;
					@SuppressWarnings("unchecked")
					ArrayList<Integer> otherRDDsp = (ArrayList<Integer>) rddshouldpersit.clone();
					otherRDDsp.remove(rddsp);
					boolean thisUse = !job.isTakeOver(otherRDDsp, rddsp);
					if(thisUse) {
						takeover = false;
						lastUse = i;
					} else {
						takeover = true;
					}
				}
			}
			result.add(result.indexOf("job "+ lastUse)+1, "unpersist "+rddsp);
		}
		correctSequence = result;
		return result;
	}
	
	public Map<Integer, String> detectBugs(){
		Map<Integer, String> report = new HashMap<Integer, String>();
		if(correctSequence.size()==0) {
			System.out.println("Generate correct sequence first please!");
			return report;
		}
		int currentJob = 0;
		for(String event: correctSequence) {
			String[] array = event.split(" ");
			String type = array[0];
			int id = Integer.parseInt(array[1]);
			switch (type) {
			case "persist":
				if(!actualSequence.contains("persist "+id))
					report.put(id, "No persist");
				else {
					int actualPos = actualSequence.indexOf("persist");
					int actualJobPos = correctSequence.indexOf("job" + currentJob);
					if (actualPos > actualJobPos)
						report.put(id, "Lagging persist");
				}
				break;
			case "unpersist":
				if(!actualSequence.contains("unpersist "+id))
					report.put(id, "No unpersist");
				else {
					int actualPos = actualSequence.indexOf("persist");
					int actualJobPos = correctSequence.indexOf("job" + currentJob);
					if(actualPos > actualJobPos)
						report.put(id, "Lagging unpersist");
					else if(actualPos < actualJobPos - 1)
						report.put(id, "Premature unpersist");
				}
				break;
			case "job":
				currentJob = id;
			default:
				break;
			}
		}
		for(String event: actualSequence) {
			String[] array = event.split(" ");
			String type = array[0];
			int id = Integer.parseInt(array[1]);
			if (!type.equals("job") && !rddshouldpersit.contains(id)) {
				report.put(id, "Unnecessary persist");
			}
		}
		detectionReport = report;
		return report;
	}
	
	public void printDetectionReport() throws Exception{
		if(detectionReport.size()==0) {
			System.out.println("Run detection process first please!");
			return;
		}
		for(Entry<Integer, String> entry: detectionReport.entrySet()) {
			int id = entry.getKey();
			String bug = entry.getValue();
			System.out.println("Bug: [" + bug + "] for RDD " + id);
		}
	}
	
	public void saveReport(String path) throws IOException {
		if(detectionReport.size()==0) {
			System.out.println("Run detection process first please!");
			return;
		}
		File savePath = new File(path);
		if(savePath.isDirectory()) {
			path += appName+".report";
			File reportFile = new File(path);
			if(reportFile.exists()) {
				reportFile.delete();
			}
			reportFile.createNewFile();
			FileOutputStream fos = new FileOutputStream(reportFile);
			OutputStreamWriter osw = new OutputStreamWriter(fos);
			for(Entry<Integer, String> entry: detectionReport.entrySet()) {
				int id = entry.getKey();
				String bug = entry.getValue();
				osw.write("Bug: [" + bug + "] for RDD " + id + "\r\n");
			}
			osw.close();
			fos.close();
		} else {
			System.out.println("Path must be the directory!");
		}
	}
}
