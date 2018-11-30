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
	    /*
	    Parse "Select" statement
	    ie: "SELECT DISTINCT persons.id FROM persons, companys WHERE persons.id = 2 ORDER BY persons.id"
	    * */
		if (!validateSelect(statement)) {
			throw new ParseException("Syntax Error!",60);
		}

		// Pattern Matcher class from java.util.regex
        // 先comment掉舒童写一半的完整版
//		Pattern r = Pattern.compile("SELECT\\s+(DISTINCT\\s+[a-z0-9.]*|[a-z0-9.]*)\\s+FROM\\s+([a-z0-9,\\s]*)(.*)");
//		Matcher m = r.matcher(statement);
//
//		if (m.find()) {
//			for (int i = 1; i <= m.groupCount(); i++) {
//				System.out.println("m.group("+i+")" + m.group(i));
//			}
//		}

        selectNode = new ParseTreeNode("SELECT");
		selectNode.setFrom(true);

        // 先从最简单的"select * from (one table)"开始
        statement = statement.trim().toLowerCase();
		//if(statement.indexOf("from") == -1) return ; //validateSelect

        List<String> attributeList = new ArrayList<>();
        List<String> tableList = new ArrayList<>();
        String[] splitResult = statement.split("select|from|where");
        System.out.println("split resutls: " + Arrays.toString(splitResult));

        // split attributes
        String[] attributes = splitResult[1].trim().replace(",", " ").split("\\s+");

        // set distinct
        boolean isDistinct = attributes[0].equalsIgnoreCase("distinct") ? true:false;
        selectNode.setDistinct(isDistinct);
        System.out.println("is distinct: " + isDistinct);
        if(isDistinct){
            attributes = Arrays.copyOfRange(attributes, 1, attributes.length);
        }
        System.out.println("attributes: " + Arrays.toString(attributes));

        // set attributes

        for(String str : attributes){
            attributeList.add(str.trim());
        }
        selectNode.setAttributes(attributeList);

        // set tablenames
        // "[\\s]*,[\\s]*" matches a comma with multiple space neighbors
        String[] tableNames = splitResult[2].trim().split("[\\s]*,[\\s]*");
        System.out.println("tablenames: " + Arrays.toString(tableNames));
        for(String table:tableNames){
            tableList.add(table.trim());
        }
        selectNode.setTablelist(tableList);

        /*
        condition expression, contains search condition and order condition
        ie: persons.id = 2 order by persons.id
        * */
        String condition = (splitResult.length == 3) ? "":splitResult[3].toLowerCase().trim();
        System.out.println("condition: " + condition);
        if(condition.trim().equals("")){
            selectNode.setWhere(false);
        }else{
            selectNode.setWhere(true);
        }
        String searchCondition, orderCondition;
        if(condition.indexOf("order by") != -1){
            selectNode.setHasOrder(true);
            String[] conditions = condition.split("order by");
            searchCondition = conditions[0].trim();
            orderCondition = conditions[1].trim();
            System.out.println("order condition: " + orderCondition);
            selectNode.setOrder_by(orderCondition);
        }else{
            selectNode.setHasOrder(false);
            searchCondition = condition;
        }
        selectNode.setSearch_condition(searchCondition);
	}

	public void parseDelete (String statement) throws ParseException{
	    /*
	    Parse "Delete" statement
	    * */
		if (!validateDelete(statement)) {
			throw new ParseException("Syntax Error!",60);
		}

		Pattern r1 = Pattern.compile("DELETE\\s+FROM\\s+([a-z][a-z0-9]*)");
		Pattern r2 = Pattern.compile("DELETE\\s+FROM\\s+([a-z][a-z0-9]*)\\s+WHERE\\s+(.*)");
        Matcher m1 = r1.matcher(statement);
		Matcher m2 = r2.matcher(statement);

		// 因为match r2的也会match r1，所以要先判断r2是否match
		if (m2.find()) {
		    // create a new parseTreeNode
			deleteNode = new ParseTreeNode("DELETE");
			// set from = true
			deleteNode.setFrom(true);
			// fill in table name
			List<String> tablelist = new ArrayList<>();
			tablelist.add(m2.group(1));
			deleteNode.setTablelist(tablelist);
			// set "where" condition
			deleteNode.setWhere(true);
			deleteNode.setSearch_condition(m2.group(2));

            System.out.println("m2:");
            for (int i = 1; i <= m2.groupCount(); i++) {
                System.out.println("m.group("+i+"): " + m2.group(i));
            }
		}
		else if(m1.find()){
		    /*
		    similar as above but don't need to set where condition
		    * */
            deleteNode = new ParseTreeNode("DELETE");
            deleteNode.setFrom(true);
            deleteNode.setWhere(false);
            List<String> tablelist = new ArrayList<>();
            tablelist.add(m1.group(1));
            deleteNode.setTablelist(tablelist);
            System.out.println("m1:");
            for (int i = 1; i <= m1.groupCount(); i++) {
                System.out.println("m.group("+i+"): " + m1.group(i));
            }
//				System.out.println("m1.group(0) = " + m1.group(0));
//				System.out.println("m1.group(1) = " + m1.group(1));

		}
	}

	public void parseCreate (String statement) throws ParseException{
	    /*
	    Parse 'Create' statement
	    ie:"CREATE TABLE persons (id INT, name STR20, name2 STR20)"
	    * */
		if (!validateCreate(statement)) {
			throw new ParseException("Syntax Error!",60);
		}

		Pattern r = Pattern.compile("CREATE\\s+TABLE\\s+([a-z][a-z0-9]*)\\s+\\((.*)\\)");
		Matcher m = r.matcher(statement);
		if (m.find()) {
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
	    /*
	    Parse "Drop" statement.
	    ie: "DROP TABLE  ss12345"
	    * */
		if (!validateDrop(statement)) {
			throw new ParseException("Syntax Error!",60);
		}
		Pattern r = Pattern.compile("DROP\\s+TABLE\\s+([a-z][a-z0-9]*)");
		Matcher m = r.matcher(statement);
		if (m.find()) {
            for (int i = 0; i <= m.groupCount(); i++) {
                System.out.println("m.group("+i+"): " + m.group(i));
            }
            /*
            m.group(0): DROP TABLE  ss12345
            m.group(1): ss12345
            * */
			dropNode = new DropNode(m.group(1));
			/*
			After above step"
			dropNode.table_name = "ss12345"
			* */
		}
	}

	public void parseInsert (String statement) throws ParseException{
	    /*
	    Parse "Insert" statement.
	    ie: "INSERT INTO persons (id,name) VALUES (12, Jerry)"
	    * */
		if (!validateInsert(statement)) {
			throw new ParseException("Syntax Error!",60);
		}

		Pattern r = Pattern.compile("INSERT\\s+INTO\\s+([a-z][a-z0-9]*)\\s+\\(([a-z0-9,\\s]*)\\)\\s+(.*)");
		Matcher m = r.matcher(statement);

		if (m.find()) {
			System.out.println(" tablename = " + m.group(1));
			System.out.println(" attributes = " + m.group(2));
			System.out.println(" other = " + m.group(3));

			insertNode = new InsertNode(m.group(1));
			String[] attributes = m.group(2).split(",");
			// ls: attributes list
			List<String> attributeList = new ArrayList<>();
			for (int i = 0; i < attributes.length; i++) {
				attributeList.add(attributes[i].trim());
				System.out.println("attribute = " + attributes[i].trim());
			}

			insertNode.setAttribute_list(attributeList);

			String other = m.group(3).trim();
			if (other.startsWith("VALUES")) {
				System.out.println("values = " + other);
				Pattern r2 = Pattern.compile("VALUES\\s+\\((.*)\\)");
				Matcher m2 = r2.matcher(other);
				if (m2.find()) {
					String[] value_arr = m2.group(1).split(",");
					// value list
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

//		//test select
		try {

			test.parseSelect("SELECT DISTINCT persons.id FROM persons, companys WHERE persons.id = 2 ORDER BY persons.id");
			//test.parseSelect("SELECT * FROM course");
            System.out.println("test.res = " + test.selectNode);
		}
		catch (Exception e) {
			System.out.println("e = " + e);
		}


//		 test drop
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
		// test delete
//		try {
//
//			test.parseDelete("DELETE FROM course WHERE sid == 1");
//			System.out.println("test.res = " + test.deleteNode);
//		}
//		catch (Exception e) {
//			System.out.println("e = " + e);
//		}

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
//		try {
//
//			test.parseCreate("CREATE TABLE persons (id INT, name STR20, name2 STR20)");
//			System.out.println("test.res = " + test.createNode);
//		}
//		catch (Exception e) {
//			System.out.println("e = " + e);
//		}

		// test insert
//
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
