package comapp;

import java.util.ArrayList;

import org.apache.commons.lang3.StringUtils;

public class Tree {
	public Node Root = null;
	public String CodIVR = null;
	public ArrayList<Node> nodes = new ArrayList<Node>();

	public Node add(String CodeNode, String CodeNodeFather, String Label, String MenuChoice, String Type, String Order) throws Exception {
		if (StringUtils.isBlank(CodeNodeFather)) {
			if (Root != null) {
				throw new Exception("More then one Root");
			}
			Root = new Node(CodeNode, CodeNodeFather, Label, MenuChoice, Type, Order);
			return Root;
		} else {
			Node node = new Node(CodeNode, CodeNodeFather, Label, MenuChoice, Type, Order);
			nodes.add(node);
			return node;
		}
	}

	@Override
	public String toString() {
		return "Tree [Root=" + Root + ", CodIVR=" + CodIVR + ", nodes=" + nodes + "]";
	}

	public void setCodIVR(String codIVR) throws Exception {
		if (StringUtils.isBlank(CodIVR) || CodIVR.equalsIgnoreCase(codIVR)) {
			CodIVR = codIVR;
		} else {
			throw new Exception("More then one CodIVR " + CodIVR + "," + codIVR);
		}
	}

	public ArrayList<Node> getChildsNode(Node father) {
		ArrayList<Node> aln = new ArrayList<>();
		if (father==null) {			
			return null;
		}
		for (Node node : nodes) {
			if (node.CodeNodeFather.equalsIgnoreCase(father.CodeNode)) {
				aln.add(node);
			}
		}
		return aln;
	}

	public ArrayList<Node> getNodesLevel(int father) {
		ArrayList<Node> aln = new ArrayList<>();
		if (father==0) {
			aln.add(Root);
			return aln;
		}
		//level 1
		
		//aln = getChildsNode(Root);
		
		//level 2
		aln.add(Root);
		ArrayList<Node> aln2 = aln;
		do {
			father--;
			aln = aln2;
			
			for (Node node : aln) {
				aln2.addAll(getChildsNode(node));
			}
		} while (father==0);
		return aln2;
	}
	
	
}
