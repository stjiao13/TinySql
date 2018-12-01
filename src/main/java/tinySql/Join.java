package main.java.tinySql;

import main.java.storageManager.*;

import java.util.ArrayList;
import java.util.List;

public class Join {

    /** Join tuple1 and tuple2 into tuple3 **/
    public Tuple joinTwoTuples(Tuple tuple1, Tuple tuple2, Tuple tuple3){
        int index = 0; // access by offset
        for(; index < tuple1.getNumOfFields(); index++) {
            String value = tuple1.getField(index).toString();
            if(isInteger(value)) { tuple3.setField(index, Integer.parseInt(value)); }
            else { tuple3.setField(index, value); }
        }
        // loop tp2, fetch its values
        index -= tuple1.getNumOfFields();
        for(; index < tuple2.getNumOfFields(); index++) {
            String value = tuple2.getField(index).toString();
            if(isInteger(value)) { tuple3.setField(index + tuple1.getNumOfFields(), Integer.parseInt(value)); }
            else { tuple3.setField(index + tuple3.getNumOfFields(), value); }
        }
        return tuple3;
    }

    /** Join two schemas **/
    public Schema joinTwoSchema(String table1, String table2, Schema schema1, Schema schema2){

        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<FieldType> fieldTpyes = new ArrayList<>();

        for(int i = 0; i < schema1.getNumOfFields(); i++){
            String fieldName = schema1.getFieldNames().get(i);
            if(!fieldName.contains(".")){
                fieldName = table1+"."+fieldName;
            }
            fieldNames.add(fieldName);
            fieldTpyes.add(schema1.getFieldTypes().get(i));
        }

        for(int j = 0; j < schema2.getNumOfFields(); j++){
            String fieldName = schema2.getFieldNames().get(j);
            if(!fieldName.contains(".")){
                fieldName = table2+"."+fieldName;
            }
            fieldNames.add(fieldName);
            fieldTpyes.add(schema2.getFieldTypes().get(j));
        }
        Schema schema3 = new Schema(fieldNames, fieldTpyes);
        return schema3;
    }

    /** Join two tables **/
    public String joinTwoTables(Main Phi, String table1, String table2){
        SchemaManager mngr = Phi.schemaManager;
        Relation relation1 = mngr.getRelation(table1);
        Relation relation2 = mngr.getRelation(table2);
        Schema schema3 = joinTwoSchema(table1, table2, relation1.getSchema(), relation2.getSchema());
        // ie: "course_cross_course2"
        String table3 = table1 + "_cross_" + table2;
        // a new empty relation
        mngr.createRelation(table3, schema3);
        Relation relation3 = mngr.getRelation(table3);
        // create a new corresponding tuple
        Tuple tuple3 = relation3.createTuple();
        // read tuples from table1 and table2, join them then insert it into table3
        for(int i = 0; i < relation1.getNumOfBlocks(); i++){
            // read block[i] from relation1 and copy it into main memory
            relation1.getBlock(i, 0);
            Block block1 = Phi.mainMemory.getBlock(0);
            if(block1.getNumTuples() == 0) continue;
            for(Tuple tuple1 : block1.getTuples()){
                // inner loop for relation 2
                // 感觉需要有优化，四个循环太慌了
                for(int j = 0; j < relation2.getNumOfBlocks(); j++){
                    // 为什么每次都要读进main memory的一个block里面。。
                    relation2.getBlock(j,2);
                    Block block2 = Phi.mainMemory.getBlock(2);
                    if(block2.getNumTuples() == 0) continue;
                    for(Tuple tuple2 : block2.getTuples()){
                        tuple3 = joinTwoTuples(tuple1, tuple2, tuple3);
                        Phi.appendTuple(relation3, Phi.mainMemory, 9, tuple3);
                    }
                }
            }
        }
        return table3;
    }


    //TODO need check!
    public List<String> joinTables(Main Phi, List<String> tables){
        // join tables without "where" clause
        List<String> tempTables = new ArrayList<>();
        String table1, table2, table3;
        int index = 0;
        for(; index < tables.size(); index ++){
            if(index == 0) {
                table1 = tables.get(index);
                table2 = tables.get(index + 1);
                table3 = joinTwoTables(Phi, table1, table2);
                tempTables.add(table3);
                index ++;
            }else{
                table1 = tempTables.get(tempTables.size()-1);
                table2 = tables.get(index);
                table3 = joinTwoTables(Phi, table1, table2);
                tempTables.add(table3);
            }
        }
        return tempTables;
    }

    /** Whether input str matches digits regex **/
    private boolean isInteger(String str){
        return str.matches("\\d+");
    }
}
