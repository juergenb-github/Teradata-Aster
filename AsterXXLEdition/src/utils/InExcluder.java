package utils;
import java.util.EmptyStackException;
import java.util.HashSet;
import java.util.List;
import java.util.Stack;
import java.util.Vector;
import java.util.regex.Pattern;

/**
 * @author Juergen Boiselle
 *
 * Include or exclude parts of a tree.
 * Walk thru the tree is done with <code>enterNode</code> and <code>exitNode</code> which evaluates if from this node on sub nodes are in- or excluded.
 * If nodes are currently included or not can be checked with <code>isIncluded</code>.
 * 
 * This class is configured by a list of excluded and included node names.
 * An item of each list either contains the local name of a node element or a regular expression.
 * A "/" as first character of the item indicates, that the node item is identified by a regular expression.
 * The regular expression is checked against the full path of the element. The full path is built out of all local names, separated by "/".
 * For example in a tree like
 * 
 * 	A
 * 		A1
 * 		A2
 * 	B
 * 		B1
 * 			B1a
 * 
 * the full path for B1a is "/B/B1/B1a/" and the regular expression is checked against this full path.
 * 
 * Each one list or both lists can be empty or null, which leads to different behavior:
 * 1. If both lists are empty or null, all nodes are included. No filtering applies.
 * 2. If only the exclude list is empty or null and the include list contains items:
 * 		No nodes are excluded by default, but all nodes matching the included items, either by direct match with the local
 * 		name or by regular expression matching with the full path, and all nodes in the sub tree below are included.
 * 3. If only the include list is empty or null and the exclude list contains items:
 * 		All nodes are included by default, except all nodes matching the excluded items, either by direct match with the local
 * 		name or by regular expression matching with the full path, and all nodes in the sub tree below are also excluded. 
 * 4. Both lists contain items:
 * 		No nodes are excluded by default, but all nodes matching the included items and all nodes in the sub tree below are included.
 * 		Up to a point where a node matches one of the items in the excluded list. From this point on the node and the sub nodes are excluded.
 * 		If on of the sub nodes is in the included list, this nodes and its sub nodes are included again. This can be done down to as many levels
 * 		as necessary.
 * 
 * A word about performance:
 * All items in the excluded and included list are checked against each node in the tree. So keep in mind that long and complex lists of
 * exclusions or inclusions will need a lot of comparisons. Direct comparison against the local name of the node is done with hash access, so quite fast,
 * whereas matching regular expression is done by matching expression by expression against node by node. This can make a huge difference. 
 */
public class InExcluder {
	private enum InOut {INCLUDE, EXCLUDE, UNCHANGED}

	// Include/Exclude items
	private CompareList included = null;
	private CompareList excluded = null;
	private CompareList skipped = null;
	private CompareList localnames = null;

	// Current status
	private Stack<NodeInfo> nodeStack = new Stack<NodeInfo>();
	private StringBuilder fullPath;
	private String includeParent;
	private int includeCount;

	public InExcluder(List<String> included, List<String> excluded, List<String> skip, List<String> localnames) {
		this.included = new CompareList(included);
		this.excluded = new CompareList(excluded);
		this.skipped = new CompareList(skip);
		this.localnames = new CompareList(localnames);
		clear();
	}
	
	// Initialize current status
	public void clear() {
		fullPath = new StringBuilder("/");
		includeParent = null;
		includeCount = 0;
		nodeStack.clear();
		nodeStack.push(new NodeInfo(included.isEmpty(), false, localnames.isEmpty(), 0));
	}
	
	public void enterNode(String localName) {
		// maintain full path
		fullPath.append(localName);
		fullPath.append("/");
		
		// Check for in- or excluded
		boolean isIncluded = isIncluded();
		InOut inOut = evaluate(localName, fullPath);
		if(inOut == InOut.INCLUDE) {
			isIncluded = true;
			includeParent = localName;
			includeCount++;
		}
		else if(inOut == InOut.EXCLUDE)
			isIncluded = false;

		// maintain node stack. Java 1.5+ efficiently reuses objects.
		nodeStack.push(new NodeInfo(isIncluded, skipped.contains(localName, fullPath), localnames.isEmpty()? true:localnames.contains(localName, fullPath), localName.length()));
	}
	
	public void exitNode() throws EmptyStackException {
		NodeInfo nodeInfo = nodeStack.pop();
		if(nodeStack.isEmpty()) throw new EmptyStackException(); // Tree is unbalanced

		fullPath.setLength(fullPath.length() - nodeInfo.getNameLen() -1);
	}
	
	public boolean isIncluded() {return nodeStack.peek().isIncluded();}
	public boolean isSkipped() {return nodeStack.peek().isSkipped();}
	public boolean matchesLocalname() {return nodeStack.peek().matchesLocalname();}
	public String getFullPath() {return fullPath.toString();}
	public String getIncludeParent() {return includeParent;}
	public int getIncludeCount() {return includeCount;}
	
	// Depending on current status of in- exclude, check if change should happen
	private InOut evaluate(String localName, StringBuilder fullPath) {
		// check for exclusion
		if(isIncluded()) {
			if(excluded.contains(localName, fullPath)) return InOut.EXCLUDE;
		}
		else { // check for inclusion
			if(included.contains(localName, fullPath)) return InOut.INCLUDE;
		}
		
		// nothing found
		return InOut.UNCHANGED;
	}
	
	// Simple container keeping node information
	private class NodeInfo {
		final boolean isIncluded;
		final boolean isSkipped;
		final boolean matchesLocalname;
		final int nameLen;
		
		public NodeInfo(boolean isIncluded, boolean isSkipped, boolean matchesLocalname, int nameLen) {
			this.isIncluded = isIncluded;
			this.isSkipped = isSkipped;
			this.matchesLocalname = matchesLocalname;
			this.nameLen = nameLen;
		}
		
		public boolean isIncluded() {return isIncluded;}
		public boolean isSkipped() {return isSkipped;}
		public boolean matchesLocalname() {return matchesLocalname;}
		public int getNameLen() {return nameLen;}
	}
	
	// keep list of local names and path and evaluate.
	private class CompareList {
		private HashSet<String> localNames = new HashSet<String>();
		private Vector<Pattern> path = new Vector<Pattern>();
		
		public CompareList(List<String> localNames) {
			if(localNames == null) return;
			for(String s : localNames) {
				if(s.startsWith("/"))
					this.path.add(Pattern.compile(s)); // Regular expression
				else
					this.localNames.add(s); // Direct comparison
			}
		}
		
		public boolean contains(String localName, StringBuilder fullPath) {
			// Direct comparison
			if(localNames.contains(localName)) return true;
			
			// Regular expression comparison
			for(Pattern pattern : path)
				if(pattern.matcher(fullPath).matches()) return true;
			
			// Not found
			return false;
		}
		
		public boolean isEmpty() {return localNames.isEmpty() && path.isEmpty();}
	}
}
