package cachecheck;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import cachecheck.Vertex;

public class DAG {

	private HashMap<Integer,Vertex> vertexMap;
	
	private Vertex youngest;
	
	public DAG(Vertex youngest) {
		this.vertexMap = new HashMap<Integer,Vertex>();
		this.youngest = youngest;
		vertexMap.put(youngest.vid, youngest);
	}
	
	public Vertex getVertex(int vid) {
		return vertexMap.getOrDefault(vid, null);
	}
	
	public void addEdge(int parent, int child) {
		if(child == youngest.vid)
			if(!youngest.parents.contains(parent))
				youngest.addParent(parent);
		if(!vertexMap.containsKey(parent))
			vertexMap.put(parent, new Vertex(parent));
		if(!vertexMap.containsKey(child))
			vertexMap.put(child, new Vertex(child));
		Vertex parentV = vertexMap.get(parent);
		Vertex childV = vertexMap.get(child);
		if(!parentV.hasChild(child))
			parentV.addChild(child);
		if(!childV.hasParent(parent))
			childV.addParent(parent);
	}
	
	/*public void removeEdge(int parent, int child) throws Exception {
		if(!vertexMap.containsKey(parent) || !vertexMap.containsKey(child))
			throw new Exception("No edge exists: "+ parent + "-" + child);
		Vertex parentV = vertexMap.get(parent);
		Vertex childV = vertexMap.get(child);
		if(parentV.hasChild(child) && childV.hasParent(parent)) {
			parentV.removeChild(child);
			childV.removeParent(parent);
		} else 
			throw new Exception("No edge exists: "+ parent + "-" + child);
	}*/
	
	public Vertex youngest() {
		return youngest;
	}
	
	public boolean hasVertex(int v) {
		return vertexMap.containsKey(v);
	}
	
	public List<Vertex> getParents(Vertex childV) {
		ArrayList<Vertex> result = new ArrayList<Vertex>();
		List<Integer> parents = childV.parents;
		for (Integer parent : parents) {
			Vertex parentV = vertexMap.get(parent);
			result.add(parentV);
		}
		return result;
	}
	
	public ArrayList<Integer> getLargestBranchPoint(DAG dag) {
		Vertex childV = dag.youngest();
		return searchBPs(childV, dag);
	}
	
	private ArrayList<Integer> searchBPs(Vertex v, DAG dag) {
		ArrayList<Integer> branchPoints = new ArrayList<Integer>();
		Integer id = v.vid;
		if (vertexMap.containsKey(id)) {
			if(!branchPoints.contains(id))
				branchPoints.add(id);
		} else {
			List<Vertex> parents = dag.getParents(v);
			for (Vertex vertex : parents) {
				branchPoints.addAll(searchBPs(vertex, dag));
			}
		}
		return branchPoints;
	}
	
	public Integer[] toArray() {
		return vertexMap.keySet().toArray(new Integer[vertexMap.size()]);
	}
	
	public boolean isUsed(ArrayList<Integer> decendants, ArrayList<Integer> childrenSPRDD, int elder) throws Exception {
		if(!vertexMap.containsKey(elder))
			throw new Exception("This job does not has rdd: " + elder);
		boolean result = searchElder(youngest, childrenSPRDD, elder); 
		for(int decendant: decendants) {
			if(hasVertex(decendant) && !childrenSPRDD.contains(decendant))
				childrenSPRDD.add(decendant);
		}
		return result;
	}
	
	private boolean searchElder(Vertex me, ArrayList<Integer> inheritors, int elder) {
		int vid = me.vid;
		if(vid==elder)
			return true;
		else if(inheritors.contains(vid)){
			return false;
		}else {
			List<Vertex> parents = getParents(me);
			for (Vertex parent : parents) {
				if(searchElder(parent, inheritors, elder))
					return true;
			}
			return false;
		}
	}
}
