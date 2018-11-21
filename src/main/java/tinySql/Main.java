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
            // TODO
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

    private void insertQuery(String stmt){
        // TODO
    }

    private void deleteQuery(String stmt){
        // TODO
    }

    public static void main(String[] args){
        String createStmt = "CREATE TABLE course (sid INT, homework INT, project INT, exam INT, grade STR20)";
        String dropStmt = "DROP TABLE  ss12345";
        Main m = new Main();
        m.exec(createStmt);
        m.exec(dropStmt);
    }
}

