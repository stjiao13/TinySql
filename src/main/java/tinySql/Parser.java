package main.java.tinySql;

import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;
/*

/**
 * Created by stjiao on 2018/11/17.
 */
public class Parser {
	// for delete and select statement, we need to represent it as a ParseTreeNode, as
	// it is rather complicated.
	// for drop,create and insert statement, we represent it as separate simple class


	public ParseTreeNode deleteNode;

	public ParseTreeNode selectNode;

	public DropNode dropNode;

	public CreateNode createNode;

	public InsertNode insertNode;


// preprocess method

	// unify the case of statement
	public String unifyCase(String statement) {

		return statement;
	}

	// validate method
	public boolean validateSelect (String statement) {

		return true;
	}

	public boolean validateDelete (String statement) {
		return true;

	}

	public boolean validateCreate (String statement) {
		return true;

	}

	public boolean validateDrop (String statement) {
		return true;

	}

	public boolean validateInsert (String statement) {
		return true;

	}

	// parsing method
	public void parseSelect (String statement) throws ParseException{
		if (!validateSelect(statement)) {
			throw new ParseException("Syntax Error!",60);
		}
		// Pattern Matcher class from java.util.regex
		Pattern r = Pattern.compile("SELECT\\s+(DISTINCT\\s+[a-z0-9.]*|[a-z0-9.]*)\\s+FROM\\s+([a-z0-9,\\s]*)(.*)");
		Matcher m = r.matcher(statement);

		if (m.find()) {
			for (int i = 1; i <= m.groupCount(); i++) {
				System.out.println("m.group("+i+")" + m.group(i));
			}
		}
	}

	public void parseDelte (String statement) throws ParseException{
		if (!validateDelete(statement)) {
			throw new ParseException("Syntax Error!",60);
		}
		Pattern r1 = Pattern.compile("DELETE\\s+FROM\\s+([a-z][a-z0-9]*)");
		Pattern r2 = Pattern.compile("DELETE\\s+FROM\\s+([a-z][a-z0-9]*)\\s+WHERE\\s+(.*)");

		Matcher m2 = r2.matcher(statement);
		if (m2.find()) {
			deleteNode = new ParseTreeNode("DELETE");
			deleteNode.setFrom(true);
			List<String> tablelist = new ArrayList<>();
			tablelist.add(m2.group(1));
			deleteNode.setTablelist(tablelist);
			deleteNode.setWhere(true);
			deleteNode.setSearch_condition(m2.group(2));
//			System.out.println("m2.group(0) = " + m2.group(0));
//			System.out.println("m2.group(1) = " + m2.group(1));
//			System.out.println("m2.group(2) = " + m2.group(2));

		}
		else {
			Matcher m1 = r1.matcher(statement);
			if (m1.find()) {

				deleteNode = new ParseTreeNode("DELETE");
				deleteNode.setFrom(true);
				List<String> tablelist = new ArrayList<>();
				tablelist.add(m1.group(1));
				deleteNode.setTablelist(tablelist);

//				System.out.println("m1.group(0) = " + m1.group(0));
//				System.out.println("m1.group(1) = " + m1.group(1));
			}
		}
	}

	public void parseCreate (String statement) throws ParseException{
		if (!validateCreate(statement)) {
			throw new ParseException("Syntax Error!",60);
		}

		Pattern r = Pattern.compile("CREATE\\s+TABLE\\s+([a-z][a-z0-9]*)\\s+\\((.*)\\)");
		Matcher m = r.matcher(statement);
		if (m.find()) {
//			for (int i = 0; i <= m.groupCount(); i++) {
//				System.out.println("i = " + i);
//				System.out.println("m.group(i) = " + m.group(i));
//
//			}

			for (int i = 0; i <= m.groupCount(); i++) {
				System.out.println("m.group("+i+"): " + m.group(i));
			}
			/*
			m.group(0): CREATE TABLE persons (id INT, name STR20, name2 STR20)
			m.group(1): persons
			m.group(2): id INT, name STR20, name2 STR20
			* */
			createNode = new CreateNode(m.group(1));

			List<String[]> attribute_type_list = new ArrayList<>();
			String[] attributes = m.group(2).split(",");
			for (String attribute :
					attributes) {
				attribute = attribute.trim();
				String[] name_type = attribute.split("\\s+");
				System.out.println("name_type: " + Arrays.toString(name_type));
				attribute_type_list.add(name_type);
			}
			createNode.setAttribute_type_list(attribute_type_list);
		}
		/*
		After above step:
		createNode.table_name = "persons"
		createNode.attribute_type_list = {[id, INT], [name, STR20], [name2, STR20]};
		* */
	}

	public void parseDrop (String statement) throws ParseException{
		if (!validateDrop(statement)) {
			throw new ParseException("Syntax Error!",60);
		}
		Pattern r = Pattern.compile("DROP\\s+TABLE\\s+([a-z][a-z0-9]*)");
		Matcher m = r.matcher(statement);
		if (m.find()) {
			dropNode = new DropNode(m.group(1));
//			System.out.println("m.group(0) = " + m.group(0));
//			System.out.println("m.group(1) = " + m.group(1));


		}
	}

	public void parseInsert (String statement) throws ParseException{
		if (!validateInsert(statement)) {
			throw new ParseException("Syntax Error!",60);
		}

		Pattern r = Pattern.compile("INSERT\\s+INTO\\s+([a-z][a-z0-9]*)\\s+\\(([a-z0-9,\\s]*)\\)\\s+(.*)");
		Matcher m = r.matcher(statement);

		if (m.find()) {
			System.out.println(" tablename = " + m.group(1));
			System.out.println(" attribute = " + m.group(2));
			System.out.println(" other = " + m.group(3));

			insertNode = new InsertNode(m.group(1));
			String[] attributes = m.group(2).split(",");
			List<String> ls = new ArrayList<>();
			for (int i = 0; i < attributes.length; i++) {
				ls.add(attributes[i].trim());
				System.out.println("attributes = " + attributes[i].trim());
			}

			insertNode.setAttribute_list(ls);

			String other = m.group(3).trim();
			if (other.startsWith("VALUES")) {
				System.out.println("values = " + other);
				Pattern r2 = Pattern.compile("VALUES\\s+\\((.*)\\)");
				Matcher m2 = r2.matcher(other);
				if (m2.find()) {
					String[] value_arr = m2.group(1).split(",");
					List<String> value_list = new ArrayList<>();

					for (int i = 0; i < value_arr.length; i++) {
						value_list.add(value_arr[i].trim());
//						System.out.println("value_arr = " + value_arr[i].trim());
					}
					insertNode.setValue_list_without_select(value_list);
				}
			}
			else {
				System.out.println("select = " + other);
				parseSelect(other);
				insertNode.setValue_lst_with_select(selectNode);
			}

		}
	}
	// test method



	public static void main(String[] args) {
		System.out.println("This is our tinysql project!!");

		Parser test = new Parser();

		//test select
//		try {
//
//			test.parseSelect("SELECT DISTINCT persons.id FROM persons, companys WHERE persons.id = 2 ORDER BY persons.id");
////			System.out.println("test.res = " + test.dropNode);
//		}
//		catch (Exception e) {
//			System.out.println("e = " + e);
//		}


		// test drop
//		try {
//
//			test.parseDrop("DROP TABLE  ss12345");
////			System.out.println("t = " + test.res.type);
////			System.out.println("test.res.getTablelist().get(0); = " + test.res.getTablelist().get(0));
//			System.out.println("test.res = " + test.dropNode);
//		}
//		catch (Exception e) {
//			System.out.println("e = " + e);
//		}
//
//
//		// test delete
//		try {
//
//			test.parseDelte("DELETE FROM tablename WHERE searchcondition");
//			System.out.println("test.res = " + test.deleteNode);
//		}
//		catch (Exception e) {
//			System.out.println("e = " + e);
//		}
//
//
//		try {
//
//			test.parseDelte("DELETE FROM tablename");
//			System.out.println("test.res = " + test.deleteNode);
//		}
//		catch (Exception e) {
//			System.out.println("e = " + e);
//		}
//
//
		// test create
		try {

			test.parseCreate("CREATE TABLE persons (id INT, name STR20, name2 STR20)");
			System.out.println("test.res = " + test.createNode);
		}
		catch (Exception e) {
			System.out.println("e = " + e);
		}

		// test insert

//		try {
//
//			test.parseInsert("INSERT INTO persons (id,name) VALUES (12, Jerry)");
//			System.out.println("test.res = " + test.insertNode);
////			test.parseInsert("INSERT INTO persons (id,name) SELECT * FROM persons");
//
//		}
//		catch (Exception e) {
//			System.out.println("e = " + e);
//		}
	}
}
