package main.java.tinySql;

//import com.sun.source.tree.ExpressionTree;

import java.util.List;

/**
 * Created by stjiao on 2018/11/18.
 */
public class ParseTreeNode {
	public String type;
	public boolean distinct;
	public List<String> attributes;
	public boolean from;
	public List<String> tablelist;
	public boolean where;

	//public ExpressionTree search_condition;
	// use string to represent ExpressionTree
	public String search_condition;
	public ParseTreeNode parent;
	public ParseTreeNode child;
	public String order_by;

	public ParseTreeNode(String type) {
		this.type = type;
	}

	public String getType() {
		return type;
	}

	public boolean isDistinct() {
		return distinct;
	}

	public List<String> getAttributes() {
		return attributes;
	}

	public boolean isFrom() {
		return from;
	}

	public List<String> getTablelist() {
		return tablelist;
	}

	public boolean isWhere() {
		return where;
	}

	public String getSearch_condition() {
		return search_condition;
	}

	public ParseTreeNode getParent() {
		return parent;
	}

	public ParseTreeNode getChild() {
		return child;
	}

	public String getOrder_by() {
		return order_by;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setDistinct(boolean distinct) {
		this.distinct = distinct;
	}

	public void setAttributes(List<String> attributes) {
		this.attributes = attributes;
	}

	public void setFrom(boolean from) {
		this.from = from;
	}

	public void setTablelist(List<String> tablelist) {
		this.tablelist = tablelist;
	}

	public void setWhere(boolean where) {
		this.where = where;
	}

	public void setSearch_condition(String search_condition) {
		this.search_condition = search_condition;
	}

	public void setParent(ParseTreeNode parent) {
		this.parent = parent;
	}

	public void setChild(ParseTreeNode child) {
		this.child = child;
	}

	public void setOrder_by(String order_by) {
		this.order_by = order_by;
	}

	@Override
	// need to modify
	public String toString() {
//		return "ParseTreeNode{" +
//				"type='" + type + '\'' +
//				", distinct=" + distinct +
//				", attributes=" + attributes +
//				", from=" + from +
//				", tablelist=" + tablelist +
//				", where=" + where +
//				", search_condition=" + search_condition +
//				", parent=" + parent +
//				", child=" + child +
//				", order_by=" + order_by +
//				'}';
		StringBuilder ans = new StringBuilder();
		ans.append(getType() + " ");
		if (isDistinct()) {
			ans.append("DISTINCT ");
		}
		if (getAttributes() != null) {
			ans.append(String.join(",", getAttributes())+" ");
		}
		if (isFrom()) {
			ans.append("FROM ");
		}
		if (getTablelist() != null) {
			ans.append(String.join(",", getTablelist())+" ");
		}

		if (isWhere()) {
			ans.append("WHERE ");
		}

		if (getSearch_condition() != null) {
			ans.append(getSearch_condition() + " ");
		}

		if (getOrder_by() != null) {
			ans.append(getOrder_by() + " ");
		}


		return ans.toString();

	}
}
