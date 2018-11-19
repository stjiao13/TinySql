package main.java.tinySql;

/**
 * Created by stjiao on 2018/11/18.
 */
public class DropNode {
	String table_name;

	public DropNode(String table_name) {
		this.table_name = table_name;
	}

	public String getTable_name() {
		return table_name;
	}

	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}

	@Override
	public String toString() {
		return "DROP TABLE " + getTable_name();
	}
}
