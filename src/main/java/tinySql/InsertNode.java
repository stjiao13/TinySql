package main.java.tinySql;

import java.util.List;

/**
 * Created by stjiao on 2018/11/18.
 */
public class InsertNode {
	public String table_name;

	public List<String> attribute_list;

	public List<String> value_list_without_select;

	public ParseTreeNode value_lst_with_select;

	public InsertNode(String table_name) {
		this.table_name = table_name;
	}

	public String getTable_name() {
		return table_name;
	}

	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}

	public List<String> getAttribute_list() {
		return attribute_list;
	}

	public void setAttribute_list(List<String> attribute_list) {
		this.attribute_list = attribute_list;
	}

	public List<String> getValue_list_without_select() {
		return value_list_without_select;
	}

	public void setValue_list_without_select(List<String> value_list_without_select) {
		this.value_list_without_select = value_list_without_select;
	}

	public ParseTreeNode getValue_lst_with_select() {
		return value_lst_with_select;
	}

	public void setValue_lst_with_select(ParseTreeNode value_lst_with_select) {
		this.value_lst_with_select = value_lst_with_select;
	}

	@Override
	public String toString() {
		StringBuilder ans = new StringBuilder();
		ans.append("INSERT INTO ");
		ans.append(getTable_name() + " (");
		ans.append(String.join(",", getAttribute_list() +" "));
		if (getValue_list_without_select() != null) {
			ans.append("VALUES ");
			ans.append(String.join(",", getValue_list_without_select()+" "));
			ans.append(")");
		}


		return ans.toString();

	}
}
