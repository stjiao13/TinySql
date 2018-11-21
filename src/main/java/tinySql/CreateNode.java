package main.java.tinySql;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by stjiao on 2018/11/18.
 */
public class CreateNode {
	public String table_name;
	public List<String[]> attribute_type_list;


	public CreateNode(String table_name) {
		this.table_name = table_name;
	}

	public List<String[]> getAttribute_type_list() {
		return attribute_type_list;
	}

	public void setAttribute_type_list(List<String[]> attribute_type_list) {
		this.attribute_type_list = attribute_type_list;
	}

	public String getTable_name() {
		return table_name;
	}

	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}

	@Override
	public String toString() {
		StringBuilder ans = new StringBuilder();
		ans.append("CREATE TABLE " + getTable_name() + " (");
		List<String> attribute_pair = new ArrayList<>();
		for (String[] pair:
			 getAttribute_type_list()) {
			attribute_pair.add(pair[0] + " " + pair[1]);
		}
//		System.out.println("String.join(\",\", attribute_pair +\" \") = " + String.join(",", attribute_pair +" "));
		ans.append(String.join(",", attribute_pair +" "));
		ans.append(")");

		return ans.toString();

	}
}
