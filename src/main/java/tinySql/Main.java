package main.java.tinySql;

import main.java.storageManager.*;

import java.io.*;
import java.util.*;

public class Main {
    Parser parser;
    MainMemory mainMemory;
    Disk disk;
    SchemaManager schemaManager;
    Join join;
    PrintWriter pw;

    public Main() throws IOException {
        parser = new Parser();
        mainMemory = new MainMemory();
        disk = new Disk();
        schemaManager = new SchemaManager(mainMemory, disk);
        disk.resetDiskIOs();
        disk.resetDiskTimer();
        join = new Join();

        pw = new PrintWriter(new FileWriter("output.txt"));
    }

    public Main(String filename) throws IOException {
        parser = new Parser();
        mainMemory = new MainMemory();
        disk = new Disk();
        schemaManager = new SchemaManager(mainMemory, disk);
        disk.resetDiskIOs();
        disk.resetDiskTimer();
        join = new Join();

        pw = new PrintWriter(new FileWriter(filename + ".txt"));
    }

    public void exec(String stmt) {
        //System.out.println("Statement: " + stmt);
        /*
        Analyse query statement then do create/drop/insert/delete/select action
        * */
        // remove duplicate spaces regex: "[\\s+]"
        String action = stmt.trim().toLowerCase().split("\\s+")[0];
        //System.out.println("Action: " + action);
        if(action.equals("create")){
            this.createQuery(stmt);
        }else if(action.equals("drop")){
            this.dropQuery(stmt);
        }else if(action.equals("insert")){
            this.insertQuery(stmt);
        }else if(action.equals("delete")){
            this.deleteQuery(stmt);
        }else if(action.equals("select")){
            this.selectQuery(stmt);
        }else {
            // throw exception
        }
    }

    public void parseFile(String file){
        BufferedReader br = null;
        try{
            FileReader fr = new FileReader(file);
            br = new BufferedReader(fr);
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
                exec(line);
            }
        } catch (IOException e){
            e.printStackTrace();
        }finally {
            try{
                br.close();
            } catch (IOException e){
                e.printStackTrace();
            }
        }
    }

    private Schema createSchema(Map<String, FieldType> map){
        /*
        Create schema:
        input: a hashmap with <key: Field Name, value: Field Type>
        output: schema
        * */
        ArrayList<String> fieldNameList = new ArrayList<>(map.keySet());
        ArrayList<FieldType> fieldTypeList = new ArrayList<>();
        for(String fieldName : fieldNameList){
            fieldTypeList.add(map.get(fieldName));
        }
        return new Schema(fieldNameList, fieldTypeList);
    }

    private void createQuery(String stmt){
        /*
        Do "create" action.
        1. parse query statement, build schema
        2. create relation (use schemaManager)
        * */
        try {
            parser.parseCreate(stmt);
            List<String[]> typeList = new ArrayList<>(parser.createNode.attribute_type_list);
            Map<String, FieldType> schemaMap = new HashMap<>();
            for(String[] pair : typeList){
                FieldType type = pair[1].toLowerCase().equals("int") ? FieldType.INT : FieldType.STR20;
                schemaMap.put(pair[0], type);
            }
            String tableName = parser.createNode.table_name;
            Schema schema = createSchema(schemaMap);
            schemaManager.createRelation(tableName, schema);
            //System.out.println("output: " + parser.createNode);
        }
        catch (Exception e) {
            System.out.println("e = " + e);
        }
    }

    private void dropQuery(String stmt){
        /*
        Do "drop" action
        1. parse query statement, get table name
        2. drop the table(delete relation)
        * */
        try {
            parser.parseDrop(stmt);
            String tableName = parser.dropNode.table_name;
            //System.out.println("Drop tble: " + tableName);
            schemaManager.deleteRelation(tableName);
            // TODO
            // pay attention: what if the table doesn't exit? catch exception?
            // deleteRelation ERROR: relation ss12345 does not exist
        }
        catch (Exception e){
            System.out.println("e= " + e);
        }
    }

    // change the method type to public so that it could be called elsewhere
    public void appendTuple(Relation relation, MainMemory mainMemory,
                             int memoryBlockNumber, Tuple tuple){
        /*
        append new tuple to relation：
        1. read blocks from main memory
        2. stores them on the disk
        * */
//        System.out.println("tuple to be appendedd: " + tuple);
//        System.out.println("before: \n" + relation.toString());
        // total number of blocks in the relation (unlimited size)
        int relationBlockNum = relation.getNumOfBlocks();
//        System.out.println("number of blocks: " + relationBlockNum);
        // get main memory block
        Block block = mainMemory.getBlock(memoryBlockNumber);
//        System.out.println("current main memory block: " + block);
//        System.out.println("num of blocks in relation: " + relationBlockNum);
        // relation is empty
        if(relationBlockNum == 0){
            // clear the block
            block.clear();
            // append tuple
            block.appendTuple(tuple);
            // reads several blocks from the memory and stores on the disk
            relation.setBlock(relation.getNumOfBlocks(), memoryBlockNumber);
        }else{
//            System.out.println("Block is full: " + block.isFull());
            // reads one block from the relation (the disk) and
            // stores in the memory
            // returns false if the index is out of bound
            relation.getBlock(relation.getNumOfBlocks() - 1, memoryBlockNumber);
            if(block.isFull()){
                // relation block is full
                block.clear();
//                System.out.println("cleared main memory block: " + mainMemory.getBlock(memoryBlockNumber));
                block.appendTuple(tuple);
                mainMemory.setBlock(memoryBlockNumber, block);
//                System.out.println("appended main memory block: " + mainMemory.getBlock(memoryBlockNumber));
                relation.setBlock(relation.getNumOfBlocks(), memoryBlockNumber);
            }else{
                block.appendTuple(tuple);
                mainMemory.setBlock(memoryBlockNumber, block);
//                System.out.println("updated block: " + block);
                relation.setBlock(relation.getNumOfBlocks()-1, memoryBlockNumber);
            }
        }
//        System.out.println("after: \n" + relation.toString());
//        System.out.println("main memory block: " + block);
    }

    private void insertQuery(String stmt){
        /*
        Do "insert" action

        case1: without "select"
        ie: "INSERT INTO course (sid,homework,project,exam,grade) VALUES (1,2,3,4,good)"
        1. parse query statement, get table name and fields
        2. create a new tuple and set fields into that tuple
        3. append tuple into the relation(disk)
        case2: with "select"
        ie: "INSERT INTO course (sid,homework,project,exam,grad) SELECT * FROM course"
        * */
        //System.out.println("Insert action:" + stmt);
        try {

            // parse insert statement
            parser.parseInsert(stmt);

            // get table name
            String tableName = parser.insertNode.table_name;

            // get relation
            Relation relation = schemaManager.getRelation(tableName);
            if(relation == null) return;

            // System.out.println("table name: " + tableName);
            // System.out.println("select: " + parser.insertNode.value_lst_with_select);
            // handle the case with "select" in insert query
            // ie: INSERT INTO course (sid, homework, project, exam, grade) SELECT * FROM course
            if(parser.insertNode.value_lst_with_select != null ){
                String tableFrom = parser.insertNode.value_lst_with_select.getTablelist().get(0);
                // System.out.println("From table: " + tableFrom);
                Relation relationFrom = schemaManager.getRelation(tableFrom);
                // attributes selected from "selected from" table
                List<String> selectedAttributes = parser.insertNode.value_lst_with_select.getAttributes();
                List<Tuple> selectedTuples = new ArrayList<>();

                if(selectedAttributes.size() == 0) return;
                clearMainMemory();
                // number of blocks in "insert to" relation
                int relationNumOfBlocks = relationFrom.getNumOfBlocks();
                int memoryNumOfBlocks = mainMemory.getMemorySize();
                List<String> selectedFieldNames = new ArrayList<>();
                if(selectedAttributes.size() == 1 && selectedAttributes.get(0).equals("*")){
                    selectedFieldNames = relationFrom.getSchema().getFieldNames();
                }else{
                    selectedFieldNames = selectedAttributes;
                }
                // TODO with DISTINCT/ORDER condition
                // Now cover Where clause
                for(int i = 0; i < relationNumOfBlocks; i++){
                    mainMemory.getBlock(0).clear();
                    relationFrom.getBlock(i, 0);
                    Block mainMemoryBlock = mainMemory.getBlock(0);
                    if(mainMemoryBlock.getNumTuples() == 0) continue;
                    for(Tuple tuple : mainMemoryBlock.getTuples()) {
                        if(parser.insertNode.value_lst_with_select.isWhere()){
                            ExpressionTree et = new ExpressionTree(parser.insertNode.value_lst_with_select.getSearch_condition());
                            if(!et.check(tuple, et.getRoot())) continue;
                        }
                        // create an empty tuple
                        Tuple newTuple = relation.createTuple();
                        // copy selected tuple's field values to new tuple
                        for(int j = 0; j < selectedFieldNames.size(); j++){
                            Field curField = tuple.getField(selectedFieldNames.get(j));
                            if(curField.type == FieldType.INT){
                                newTuple.setField(selectedFieldNames.get(j), curField.integer);
                            }else{
                                newTuple.setField(selectedFieldNames.get(j), curField.str);
                            }
                        }
                        // append new tuple to relation
                        // System.out.println("new tuple: " + newTuple);
                        appendTuple(relation, mainMemory, 0, newTuple);
                    }
                }
                return;
            }


            // create a new tuple
            Tuple tuple = relation.createTuple();
            Schema schema = relation.getSchema();

            // get filed names and corresponding values
            List<String> filedNames = new ArrayList<>(parser.insertNode.getAttribute_list());
            List<String> values = new ArrayList<>(parser.insertNode.getValue_list_without_select());

            // set fields into that tuple
            for(int i = 0; i < filedNames.size(); i++){
                String filedName = filedNames.get(i);
                String value = values.get(i);
                if(value.matches("^-?\\d+$")){
                    tuple.setField(filedName, Integer.parseInt(value));
                }else{
                    tuple.setField(filedName, value);
                }
            }
            //System.out.println("new tuple: " + tuple);
            appendTuple(relation, mainMemory, 0, tuple);
        }
        catch (Exception e){
            System.out.println("e= " + e);
        }
    }

    private void deleteQuery(String stmt){
        /*
        Do "DELETE" action:
        1. parser statement and get tuples which will be deleted
        2. copy the block to memory, invalidate the tuple and write the modified
           memory block back to the relation block R.
        * */
        try {
            parser.parseDelete(stmt);

            // get table name and corresponding relation
            List<String> tableList = parser.deleteNode.getTablelist();
            String tableName = tableList.get(0);
            Relation relation = schemaManager.getRelation(tableName);

            String searchCondition = parser.deleteNode.getSearch_condition();
            // total number of blocks in the memory (max:10)
            int memoryBlockNum = mainMemory.getMemorySize();
            // total number of blocks in the relation (unlimited size)
            int relationBlockNum = relation.getNumOfBlocks();
            // relation block index

            int curBlockIdx = 0;
            // check all blocks of this relation by offset 0,1,2..
            while(relationBlockNum > 0){
                // can't exceed max memory size or num of remaining blocks in disk
                int numOfBlockToMem = Math.min(memoryBlockNum, relationBlockNum);
                // reads several blocks from the relation (the disk) and
                // stores in the memory
                // returns false if the index is out of bound
                relation.getBlocks(curBlockIdx, 0, numOfBlockToMem);

                // after copy disk blocks to main memory,
                // we can read and modify tuples
                for(int i = 0; i < numOfBlockToMem; i++){
                    Block block = mainMemory.getBlock(i);
                    if(block.getNumTuples() == 0){
                        // empty block
                        continue;
                    }
                    List<Tuple> tuples = block.getTuples();
                    //System.out.println("delete is where: " + parser.deleteNode.isWhere());
                    if(parser.deleteNode.isWhere()){
                        for(int j = 0; j < tuples.size(); j++){
                            // construct search condition into an expression tree
                            //System.out.println("where condition: " + searchCondition);
                            ExpressionTree tree = new ExpressionTree(searchCondition);
                            //System.out.println("E tree: " + tree.toString(tree.getRoot()));
                            Tuple tuple = tuples.get(j);
                            //System.out.println("current tuple: " + tuple);
                            //System.out.println("whether satisfy where condition: " + tree.check(tuple, tree.getRoot()));
                            if(tree.check(tuple, tree.getRoot())){
                                //System.out.println("Tuple to be invalidated: " + tuple);
                                // invalidate tuples which satisfy search condition
                                block.invalidateTuple(j);
                            }
                        }
                    }else{
                        // invalidate all tuples
                        //System.out.println("Block to be invalidated: " + block);
                        block.invalidateTuples();
                    }
                }
                // write modified blocks into disk
                relation.setBlocks(curBlockIdx, 0, numOfBlockToMem);
                relationBlockNum -= numOfBlockToMem;
                curBlockIdx += numOfBlockToMem;
            }
        }catch (Exception e){
            System.out.println("e= " + e);
        }
    }

    private void selectQuery(String stmt){
        try{
            // update select parser note
            parser.parseSelect(stmt);
            List<String> tableList = parser.selectNode.getTablelist();
            //System.out.println("table list: " + tableList);
            //System.out.println("selected attributes: " + parser.selectNode.getAttributes());
            if(tableList.size() == 1){
                selectQuery1();
            }else{
                selectQuery2();
            }
        }
        catch (Exception e){
            System.out.println("e= " + e);
        }
    }

    /** Select multiple talbes **/
    private void selectQuery2(){
        List<String> tableList = parser.selectNode.getTablelist();
        List<String> tempTables = new ArrayList<>();
        if(parser.selectNode.isWhere()){
            //System.out.println("multi table with where: " + tableList);
            ExpressionTree tree = new ExpressionTree(parser.selectNode.search_condition);
            //System.out.println("tree: " + tree.toString(tree.getRoot()));
            clearMainMemory();
            tempTables = join.joinTables(this, tableList, tree);
            // System.out.println("new tables: " + tempTables);
        }else{
            //System.out.println("query2 table list: " + tableList);
            tempTables = join.joinTables(this, tableList);
            //System.out.println("temp tables: " + tempTables);
        }

        String table = tempTables.get(tempTables.size()-1);
        parser.selectNode.setTablelist(new ArrayList<>(Arrays.asList(table)));
        //System.out.println("new table list: " + parser.selectNode.getTablelist());
        selectQuery1();
        join.DropTables(this, tempTables);
    }
  
    private void selectQuery1(){
        /*
        Do "SELECT" action on only one table
        case : "select [distinct] (attributes or *) from (one table) where []"
        * */
        //
        try{
            // get table name and corresponding relation
            List<String> tableList = parser.selectNode.getTablelist();
            String tableName = tableList.get(0);
            Relation relation = schemaManager.getRelation(tableName);
            // get selected attributes
            List<String> selectedAttributes = parser.selectNode.getAttributes();
            List<Tuple> selectedTuples = new ArrayList<>();
            // selected fields which will be printed out
            List<String> selectedFieldNames = new ArrayList<>();
            if(relation == null || selectedAttributes.size() == 0){
                // relation doesn't exit or any attribute is selected
                return;
            }
            // total number of blocks in the relation (unlimited size)
            int relationBlockNum = relation.getNumOfBlocks();
            // total number of blocks in the memory (max:10)
            int memoryBlockNum = mainMemory.getMemorySize();

            if(selectedAttributes.size() == 1 && selectedAttributes.get(0).equals("*")){
                // "select *" case
                selectedFieldNames = relation.getSchema().getFieldNames();
            }else{
                // select attributes
                selectedFieldNames = selectedAttributes;

            }

            if(!parser.selectNode.isDistinct() && !parser.selectNode.isOrder()){
                // System.out.println("has where? " + parser.selectNode.getSearch_condition());
                show(parser.selectNode, relation, selectedFieldNames);
                return;
            }

            for(int i = 0; i < relationBlockNum; i++){
                // reads ONE block from the relation (the disk) and
                // stores in the memory
                // returns false if the index is out of bound
                relation.getBlock(i, 0);
                Block mainMemoryBlock = mainMemory.getBlock(0);
                if(mainMemoryBlock.getNumTuples() == 0){
                    continue;
                }

                // select tuples which satisfy "where" condition
                //System.out.println("is where: " + parser.selectNode.isWhere());
                if(parser.selectNode.isWhere()){
                    for(Tuple tuple : mainMemoryBlock.getTuples()){
                        ExpressionTree tree = new ExpressionTree(parser.selectNode.search_condition);
                        if(tree.check(tuple, tree.getRoot())){
                            selectedTuples.add(tuple);
                        }
                    }
                }else{
                    selectedTuples.addAll(mainMemoryBlock.getTuples());
                }

            }

            // if is distinct, then eliminate duplicate tuples
            // override tuple's hashcode
            //System.out.println("is distinct: " + parser.selectNode.isDistinct());
            if(parser.selectNode.isDistinct()){
                Set<uniqueTuple> set = new HashSet<>();
                int i = 0;
                while(i < selectedTuples.size()){
                    Tuple tuple = selectedTuples.get(i);
                    uniqueTuple utuple = new uniqueTuple(tuple, selectedFieldNames);
                    if(!set.add(utuple)){
                        selectedTuples.remove(i);
                        i --;
                    }
                    i++;
                }
            }

            // sort selected tuples by order
            if(parser.selectNode.isOrder()){
                Comparator<Tuple> comp = new Comparator<Tuple>() {
                    @Override
                    public int compare(Tuple o1, Tuple o2) {
                        Field field1 = o1.getField(parser.selectNode.getOrder_by());
                        Field field2 = o2.getField(parser.selectNode.getOrder_by());
                        if(field1.type == FieldType.STR20){
                            return field1.str.compareTo(field2.str);
                        }else{
                            return ((Integer)field1.integer).compareTo((Integer)field2.integer);
                        }
                    }
                };
                Collections.sort(selectedTuples, comp);
            }
            // handle case like "course.id"
            if(!tableName.contains("_cross_")) {
                List<String> new_selectedFieldNames = new ArrayList<>();
                for(String fieldName: selectedFieldNames){
                    if(fieldName.contains(".")){
                        new_selectedFieldNames.add(fieldName.split("\\.")[1]);
                    }else{
                        new_selectedFieldNames.add(fieldName);
                    }
                }
            }
            show(parser.selectNode, selectedFieldNames, selectedTuples);
        }
        catch (Exception e){
            System.out.println("e= " + e);
        }
    }

//    private void outputTuples(List<String> selectedFieldNames, List<Tuple> selectedTuples){
//        // print tuples
//        if(selectedTuples.size() == 0){
//            System.out.println("No Tuple Found!");
//            return;
//        }
//        String sb;
//        sb = String.join("\t", selectedFieldNames) + "\n";
//        for(Tuple tuple : selectedTuples){
//            for(String attri:selectedFieldNames){
//                sb += (tuple.getField(attri) + "\t");
//            }
//            sb += "\n";
//        }
//        System.out.println(sb);
//    }

    /**
     * Print out results which satisfy where condition.
     * Be able to eliminate duplicates if there is a "distinction" requirement
     * **/
    private void show(ParseTreeNode parseTreeNode, Relation relation, List<String> selectedFieldNames){
        int relationNumOfBlocks = relation.getNumOfBlocks();
        int numberOfRows = 0;
        String prev = "nullPrev";
        if(parseTreeNode.getTablelist() == null || parseTreeNode.getTablelist().size() == 0) return;
        String tableName = parseTreeNode.getTablelist().get(0);
        // print column headers
        if(tableName.indexOf("_cross_") == -1 && tableName.indexOf("_natureJoin_") == -1){
            for(int i = 0; i < selectedFieldNames.size(); i++){
                String selectedFieldName = selectedFieldNames.get(i);
                if(selectedFieldName.indexOf(".") != -1) {
                    selectedFieldNames.set(i, selectedFieldName.split("\\.")[1]);
                }
                System.out.print(selectedFieldNames.get(i) + "\t");
                pw.print(selectedFieldNames.get(i) + "\t");
            }
        }
        System.out.println();
        pw.println();

        for(int i = 0; i < relationNumOfBlocks; i++){
            // read a block from disk to main memory[0]
            relation.getBlock(i, 0);
            Block mainMemoryBlock = mainMemory.getBlock(0);
            if(mainMemoryBlock.getNumTuples() == 0){
                continue;
            }
            for(Tuple tuple : mainMemoryBlock.getTuples()){
                if(tuple.isNull()) continue;
                if(parseTreeNode.isWhere()){
                    ExpressionTree tree = new ExpressionTree(parseTreeNode.search_condition);
                    // System.out.println("tree built: " + tree.toString(tree.getRoot()));
                    if(!tree.check(tuple, tree.getRoot())) continue;
                }
                StringBuilder sb = new StringBuilder();
                for(String field : selectedFieldNames){
                    String val = tuple.getField(field).toString();
                    if(val.equals("-2147483648") || val.equals("null")){
                        val = "NULL";
                    }
                    sb.append(val).append("\t");
                }
                System.out.println();
                pw.println();
                String cur = sb.toString();
                if(parseTreeNode.isDistinct() && cur.equalsIgnoreCase(prev)) continue;
                prev = cur;
                System.out.println(cur);
                pw.println(cur);
                numberOfRows ++;
            }

        }
        System.out.println("----------------------------------");
        System.out.println(numberOfRows + " rows of results");
        System.out.println();
        pw.println("----------------------------------");
        pw.println(numberOfRows + " rows of results");
        pw.println();

    }

    /**
     * Override show(**args) function.
     * Selectedtuples: list of tuples which already satisfy parseTreeNode's search condition
     * **/
    private void show(ParseTreeNode parseTreeNode, List<String> selectedFieldNames, List<Tuple> selectedTuples){
        int numberOfRows = 0;
        String prev = "nullPrev";
        if(parseTreeNode.getTablelist() == null || parseTreeNode.getTablelist().size() == 0) return;
        String tableName = parseTreeNode.getTablelist().get(0);
        // print column headers

        if(tableName.indexOf("_cross_") == -1){
            for(int i = 0; i < selectedFieldNames.size(); i++){
                String selectedFieldName = selectedFieldNames.get(i);
                if(selectedFieldName.indexOf(".") != -1) {
                    selectedFieldNames.set(i, selectedFieldName.split("\\.")[1]);
                }
                System.out.print(selectedFieldNames.get(i) + "\t");
                pw.print(selectedFieldNames.get(i) + "\t");
            }
        }
        System.out.println();
        pw.println();
        for(Tuple tuple : selectedTuples){
            //if(tuple.isNull()) continue;
            StringBuilder sb = new StringBuilder();
            for(String field : selectedFieldNames){
                String val = tuple.getField(field).toString();
                if(val.equals("-2147483648") || val.equals("null")){
                    val = "NULL";
                }
                sb.append(val).append("\t");
            }
            String cur = sb.toString();
            if(parseTreeNode.isDistinct() && cur.equals(prev)) continue;
            prev = cur;
            System.out.println(cur);
            System.out.println(cur);
            numberOfRows ++;
        }
        System.out.println("----------------------------------");
        System.out.println(numberOfRows + " rows of results");
        System.out.println();
        pw.println("----------------------------------");
        pw.println(numberOfRows + " rows of results");
        pw.println();
    }

    public void clearMainMemory(){
        int numOfBlocks = mainMemory.getMemorySize();
        for(int i = 0; i < numOfBlocks; i++){
            mainMemory.getBlock(i).clear();
        }
    }

    public static void main (String[] args) throws IOException  {
//        String createStmt1 = "CREATE TABLE course (sid INT, homework INT, project INT, exam INT, grade STR20)";
//        String createStmt2 = "CREATE TABLE course2 (sid INT, exam INT, grade STR20)";
//        //String dropStmt = "DROP TABLE  ss12345";
//        String insertStmt1 = "INSERT INTO course (sid,homework,project,exam,grade) VALUES (1,2,3,4,good)";
//        String insertStmt11 = "INSERT INTO course (sid,homework,project,exam,grade) VALUES (2,0,5,1,bad)";
//        String insertStmt2 = "INSERT INTO course2 (sid, exam, grade) VALUES (1, 100, A)";
//        String selectStmt1 = "SELECT * FROM course";
//        String selectStmt2 = "SELECT * FROM course2";
//        Main m = new Main();
//        m.exec(createStmt1);
//        m.exec(createStmt2);
//        for(int i = 0; i < 4; i++){
//            m.exec(insertStmt1);
//        }
//        m.exec(insertStmt1);
//        m.exec(insertStmt11);
//        m.exec(insertStmt1);
//        m.exec(insertStmt11);
//        m.exec(selectStmt1);
        //m.exec(selectStmt2);
          Main main = new Main("outputOfOriginalTest");
          long startTime = System.nanoTime();
          main.parseFile("test_origin.txt");
          long endTime = System.nanoTime();
          System.out.println("Time Used: " + (endTime - startTime)/1000000000 + "s");
    }
}

