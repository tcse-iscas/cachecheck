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
	private Map<String, String> detectionReport;
	private String appName = "default";
	
	public Util(String traceFilePath, String jobFilePath, String appName) {
		jobs = new ArrayList<DAG>();
		rddshouldpersit = new ArrayList<Integer>();
		actualSequence = new ArrayList<String>();
		correctSequence = new ArrayList<String>();
		this.appName = appName;
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
	
	private boolean isLastJob(int jobId, ArrayList<String> sequence) {
		boolean result = false;
		for(String event : sequence) {
			if(event.equals("job "+jobId))
				result = true;
			else if(result)
				return false;
		}
		return result;
	}

	public Map<String, String> detectBugs() throws Exception{
		Map<String, String> report = new HashMap<String, String>();
		if(correctSequence.size()==0) {
			System.out.println("Generate correct sequence first please!");
			return report;
		}
		int lastJob = -1;
		int nextJob = 0;
		for(String event: correctSequence) {
			String[] array = event.split(" ");
			String type = array[0];
			int id = Integer.parseInt(array[1]);
			switch (type) {
			case "persist":
				if(!actualSequence.contains("persist "+id))
					report.put("p-"+id, "No persist");
				else {
					int actualPos = actualSequence.indexOf("persist "+id);
					int nextJobPos = actualSequence.indexOf("job " + nextJob);
					// If persist is after the next job, it is a lagging persist.
					// nextJobPos can be -1, which means it is the last job.
					if (actualPos > nextJobPos)
						report.put("p-"+id, "Lagging persist");
				}
				break;
			case "unpersist":
				// If the rdd is a no persist bug, no unpersist should not be a bug
				if((!actualSequence.contains("unpersist "+id)) && actualSequence.contains("persist "+id)) {
					// If the correct unpersist is after the last job, it is not a bug 
					if(!isLastJob(nextJob, correctSequence)) 
						report.put("up-"+id, "No unpersist");
				} else if(actualSequence.contains("unpersist "+id)) {
					int actualPos = actualSequence.indexOf("unpersist "+id);
					int lastJobPos = actualSequence.indexOf("job " + lastJob);
					int nextJobPos = actualSequence.indexOf("job " + nextJob);
					// If unpersist is after the next job, it is a lagging unpersist.
					// The next job can be non-existent (nextJobPos == -1),
					// because it is after the last job. This situation is not a bug.
					if(actualPos > nextJobPos && nextJobPos > 0)
						report.put("up-"+id, "Lagging unpersist");
					// If unpersist is before the last job, 
					// it is a premature unpersist
					else if(actualPos < lastJobPos)
						report.put("up-"+id, "Premature unpersist");
				}
				break;
			case "job":
				lastJob = id;
				nextJob = id + 1;
			default:
				break;
			}
		}
		for(String event: actualSequence) {
			String[] array = event.split(" ");
			String type = array[0];
			int id = Integer.parseInt(array[1]);
			if (!type.equals("job") && !rddshouldpersit.contains(id)) {
				report.put("p-"+id, "Unnecessary persist");
			}
		}
		detectionReport = report;
		return report;
	}
	
	public void printDetectionReport() throws Exception{
		if(null == detectionReport) {
			System.out.println("Run detection process first please!");
			return;
		}
		for(Entry<String, String> entry: detectionReport.entrySet()) {
			int id = Integer.parseInt(entry.getKey().split("-")[1]);
			String bug = entry.getValue();
			System.out.println("Bug: [" + bug + "] for RDD " + id);
		}
	}
	
	public void saveReport(String path) throws IOException {
		if(null == detectionReport) {
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
			for(Entry<String, String> entry: detectionReport.entrySet()) {
				int id = Integer.parseInt(entry.getKey().split("-")[1]);
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
