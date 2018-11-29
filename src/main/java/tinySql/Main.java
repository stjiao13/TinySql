package main.java.tinySql;

import main.java.storageManager.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    Parser parser;
    MainMemory mainMemory;
    Disk disk;
    SchemaManager schemaManager;

    public Main(){
        parser = new Parser();
        mainMemory = new MainMemory();
        disk = new Disk();
        schemaManager = new SchemaManager(mainMemory, disk);
        disk.resetDiskIOs();
        disk.resetDiskTimer();
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
            System.out.println(tableName);
            schemaManager.deleteRelation(tableName);
            // TODO
            // pay attention: what if the table doesn't exit? catch exception?
            // deleteRelation ERROR: relation ss12345 does not exist
        }
        catch (Exception e){
            System.out.println("e= " + e);
        }
    }
    private void appendTuple(Relation relation, MainMemory mainMemory,
                             int memoryBolckNumber, Tuple tuple){
        /*
        append new tuple to relation
        * */
        int numOfBlocks = relation.getNumOfBlocks();
        System.out.println("number of blocks: " + numOfBlocks);
        // get main memory block
        Block block = mainMemory.getBlock(memoryBolckNumber);
        if(numOfBlocks == 0){
            // relation is empty
            block.clear();
            block.appendTuple(tuple);
            relation.setBlock(relation.getNumOfBlocks(), memoryBolckNumber);
        }else{
            relation.getBlock(relation.getNumOfBlocks() - 1, memoryBolckNumber);
            if(block.isFull()){
                // relation block is full
                block.clear();
                block.appendTuple(tuple);
                relation.setBlock(relation.getNumOfBlocks(), memoryBolckNumber);
            }else{
                block.appendTuple(tuple);
                relation.setBlock(relation.getNumOfBlocks()-1, memoryBolckNumber);
            }
        }
    }

    private void insertQuery(String stmt){
        // TODO
        /*
        Do "insert" action

        case1: without "select"
        ie: "INSERT INTO course (sid,homework,project,exam,grade) VALUES (1,2,3,4,good)"
        1. parse query statement, get table name and fields
        2. create a new tuple and set fields into that tuple

        case2: with "select"
        TODO
        * */
        System.out.println("Insert action:");
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
            appendTuple(relation, mainMemory, 5, tuple);
        }
        catch (Exception e){
            System.out.println("e= " + e);
        }
    }

    private void deleteQuery(String stmt){
        /*
        Do "DELETE" action
        * */
        try {
            parser.parseDelete(stmt);

            // get table name and corresponding relation
            List<String> tableList = parser.deleteNode.getTablelist();
            String tableName = tableList.get(0);
            Relation relation = schemaManager.getRelation(tableName);

            String searchConditin = parser.deleteNode.getSearch_condition();

            int memoryBlockNum = mainMemory.getMemorySize();
            int relationBlockNum = relation.getNumOfBlocks();
            // relation block index
            int curBlockIdx = 0;
            while(relationBlockNum > 0){
                // check all blocks of this relation in disk
                int numOfBlockToMem = Math.min(memoryBlockNum, relationBlockNum);
                relation.getBlocks(curBlockIdx, 0, numOfBlockToMem);
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
                            ExpressionTree tree = new ExpressionTree(searchConditin);
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
                relation.setBlocks(curBlockIdx, 0, numOfBlockToMem);
                numOfBlockToMem -= numOfBlockToMem;
                curBlockIdx += numOfBlockToMem;
            }
        }catch (Exception e){
            System.out.println("e= " + e);
        }
    }

    private void selectQuery(String stmt){
        /*
        Do "SELECT" action
        case 1: 先写 "select (attributes or *) from (one table)"这种情况
        * */
        //
        try{
            // update select parser note
            parser.parseSelect(stmt);

            // get table name and corresponding relation
            List<String> tableList = parser.selectNode.getTablelist();
            String tableName = tableList.get(0);
            Relation relation = schemaManager.getRelation(tableName);

            // get selected attributes
            List<String> selectedAttributes = parser.selectNode.getAttributes();
            List<Tuple> selectedTuples = new ArrayList<>();
            List<Field> selectedFields = new ArrayList<>();
            if(relation == null || selectedAttributes.size() == 0){
                // relation doesn't exit or any attribute is selected
                return;
            }
            int relationBlockNum = relation.getNumOfBlocks();
            int memoryBlockNum = mainMemory.getMemorySize();

            if(selectedAttributes.size() == 1 && selectedAttributes.get(0).equals("*")){
                // "select *" case
                List<String> fieldNameList = relation.getSchema().getFieldNames();
                for(int i = 0; i < relationBlockNum; i++){
                    relation.getBlock(i, 0);
                    Block mainMemoryBlock = mainMemory.getBlock(0);
                    if(mainMemoryBlock.getNumTuples() == 0){
                        continue;
                    }
                    selectedTuples.addAll(mainMemoryBlock.getTuples());
                }
                // output tuples
                for(Tuple tuple : selectedTuples){
                    System.out.println(tuple);
                }
            }else{
                // TODO
                // "select attributes" case

            }
        }
        catch (Exception e){
            System.out.println("e= " + e);
        }
    }

    public static void main(String[] args){
        String createStmt = "CREATE TABLE course (sid INT, homework INT, project INT, exam INT, grade STR20)";
        //String dropStmt = "DROP TABLE  ss12345";
        String insertStmt = "INSERT INTO course (sid,homework,project,exam,grade) VALUES (1,2,3,4,good)";
        String selectStmt = "SELECT * FROM course";
        Main m = new Main();
        m.exec(createStmt);
        for(int i = 0; i < 4; i++){
            m.exec(insertStmt);
        }
        m.exec(selectStmt);

    }
}

