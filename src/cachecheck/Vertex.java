package cachecheck;

import java.util.ArrayList;
import java.util.List;

public class Vertex {
	int vid;
	List<Integer> children;
	List<Integer> parents;
	public Vertex(int id) {
		// TODO Auto-generated constructor stub
		vid = id;
		children = new ArrayList<Integer>();
		parents = new ArrayList<Integer>();
	}
	
	public int addParent(int parent) {
		parents.add(parent);
		return parent;
	}
	
	public int addChild(int child) {
		children.add(child);
		return child;
	}
	
	public int removeParent(int parent) {
		parents.remove(parent);
		return parent;
	}
	
	public int removeChild(int child) {
		children.remove(child);
		return child;
	}
	
	public boolean isAction() {
		return children.isEmpty();
	}
	
	public boolean hasChild(int child) {
		return children.contains(child);
	}
	
	public boolean hasParent(int parent) {
		return parents.contains(parent);
	}
	
	public boolean hasParents() {
		return !parents.isEmpty();
	}
}
