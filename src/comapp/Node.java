package comapp;



public class Node {

	public String CodeNode;
	public String CodeNodeFather;
	public String Label;
	public String MenuChoice;
	public String Type;
	public String Order;
	public String Message;
	

	public Node(String codeNode, String codeNodeFather, String label, String menuChoice, String type, String order) {
		super();
		CodeNode = codeNode;
		CodeNodeFather = codeNodeFather;
		Label = label;
		MenuChoice = menuChoice;
		Type = type;
		Order = order;
		
	}


	@Override
	public String toString() {
		return "Node [CodeNode=" + CodeNode + ", CodeNodeFather=" + CodeNodeFather + ", Label=" + Label + ", MenuChoice=" + MenuChoice + ", Type=" + Type + ", Order=" + Order + ", Message=" + Message + "]";
	}

}
