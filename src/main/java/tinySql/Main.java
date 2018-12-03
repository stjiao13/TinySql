package main.java.tinySql;

import main.java.storageManager.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Main {
    Parser parser;
    MainMemory mainMemory;
    Disk disk;
    SchemaManager schemaManager;
    Join join;

    public Main(){
        parser = new Parser();
        mainMemory = new MainMemory();
        disk = new Disk();
        schemaManager = new SchemaManager(mainMemory, disk);
        disk.resetDiskIOs();
        disk.resetDiskTimer();
        join = new Join();
    }

    public void exec(String stmt) {
        /*
        Analyse query statement then do create/drop/insert/delete/select action
        * */
        // remove duplicate spaces regex: "[\\s+]"
        String action = stmt.trim().toLowerCase().split("\\s+")[0];
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
        }else{
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
            System.out.println("output: " + parser.createNode);
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
            System.out.println("Drop tble: " + tableName);
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

        // total number of blocks in the relation (unlimited size)
        int relationBlockNum = relation.getNumOfBlocks();
        System.out.println("number of blocks: " + relationBlockNum);
        // get main memory block
        Block block = mainMemory.getBlock(memoryBlockNumber);

        // relation is empty
        if(relationBlockNum == 0){
            // clear the block
            block.clear();
            // append tuple
            block.appendTuple(tuple);
            // reads several blocks from the memory and stores on the disk
            relation.setBlock(relation.getNumOfBlocks(), memoryBlockNumber);
        }else{
            relation.getBlock(relation.getNumOfBlocks() - 1, memoryBlockNumber);
            if(block.isFull()){
                // relation block is full
                block.clear();
                block.appendTuple(tuple);
                relation.setBlock(relation.getNumOfBlocks(), memoryBlockNumber);
            }else{
                block.appendTuple(tuple);
                relation.setBlock(relation.getNumOfBlocks()-1, memoryBlockNumber);
            }
        }
        System.out.println("main memory block: " + block);
    }

    private void insertQuery(String stmt){
        // TODO
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
        System.out.println("Insert action:" + stmt);
        try {
            // parse insert statement
            parser.parseInsert(stmt);

            // get table name
            String tableName = parser.insertNode.table_name;

            // get relation
            Relation relation = schemaManager.getRelation(tableName);
            if(relation == null) return;

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
            System.out.println("new tuple: " + tuple);
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
                    if(parser.deleteNode.isWhere()){
                        for(int j = 0; j < tuples.size(); j++){
                            // construct search condition into an expression tree
                            ExpressionTree tree = new ExpressionTree(searchCondition);
                            Tuple tuple = tuples.get(j);
                            if(tree.check(tuple, tree.getRoot())){
                                // invalidate tuples which satisfy search condition
                                block.invalidateTuple(j);
                            }
                        }
                    }else{
                        // invalidate all tuples
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
            System.out.println("table list: " + tableList);
            System.out.println("selected attributes: " + parser.selectNode.getAttributes());
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

    /** Select multiple talbes:
     *  first cross join tables then use select the joined table **/
    private void selectQuery2(){
        List<String> tableList = parser.selectNode.getTablelist();
        List<String> tempTables = new ArrayList<>();
        if(parser.selectNode.isWhere()){
            ExpressionTree tree = new ExpressionTree(parser.selectNode.search_condition);
           tempTables = join.joinTables(this, tableList, tree);
        }else{
            System.out.println("query2 table list: " + tableList);
            // stuck here
            tempTables = join.joinTables(this, tableList);
            System.out.println("temp tables: " + tempTables);
        }

        String table = tempTables.get(tempTables.size()-1);
        parser.selectNode.setTablelist(new ArrayList<>(Arrays.asList(table)));
        System.out.println("new table list: " + parser.selectNode.getTablelist());
        selectQuery1();
        join.DropTables(this, tempTables);
    }
  
    private void selectQuery1(){
        /*
        Do "SELECT" action
        case : "select (attributes or *) from (one table)"
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
            List<Field> selectedFields = new ArrayList<>();
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
                System.out.println("is where: " + parser.selectNode.isWhere());
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

            // if is distinct, then remove duplicate tuples
            // override tuple's hashcode
            System.out.println("is distinct: " + parser.selectNode.isDistinct());
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
            outputTuples(selectedFieldNames, selectedTuples);
        }
        catch (Exception e){
            System.out.println("e= " + e);
        }
    }

    private void outputTuples(List<String> selectedFieldNames, List<Tuple> selectedTuples){
        // print tuples
        if(selectedTuples.size() == 0){
            System.out.println("No Tuple Found!");
            return;
        }
        String sb;
        sb = String.join("\t", selectedFieldNames) + "\n";
        for(Tuple tuple : selectedTuples){
            for(String attri:selectedFieldNames){
                sb += (tuple.getField(attri) + "\t");
            }
            sb += "\n";
        }
        System.out.println(sb);
    }

    public void clearMainMemory(){
        int numOfBlocks = mainMemory.getMemorySize();
        for(int i = 0; i < numOfBlocks; i++){
            mainMemory.getBlock(i).clear();
        }
    }

    public static void main(String[] args){
        String createStmt1 = "CREATE TABLE course (sid INT, homework INT, project INT, exam INT, grade STR20)";
        String createStmt2 = "CREATE TABLE course2 (sid INT, exam INT, grade STR20)";
        //String dropStmt = "DROP TABLE  ss12345";
        String insertStmt1 = "INSERT INTO course (sid,homework,project,exam,grade) VALUES (1,2,3,4,good)";
        String insertStmt2 = "INSERT INTO course2 (sid, exam, grade) VALUES (1, 100, A)";
        String selectStmt1 = "SELECT * FROM course2, course";
        String selectStmt2 = "SELECT * FROM course2";
        Main m = new Main();
        m.exec(createStmt1);
        m.exec(createStmt2);
        for(int i = 0; i < 4; i++){
            m.exec(insertStmt1);
        }
        m.exec(insertStmt2);
        m.exec(selectStmt1);
        m.exec(selectStmt2);
//          Main main = new Main();
//          long startTime = System.nanoTime();
//          main.parseFile("test.txt");
//          long endTime = System.nanoTime();
//          System.out.println("Time Used: " + (endTime - startTime)/1000000000 + "s");
    }
}

