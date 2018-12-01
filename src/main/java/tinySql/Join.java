package main.java.tinySql;

import main.java.storageManager.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    /** Join two tables with where condition **/
    public String joinTwoTables(Main Phi, String table1, String table2,
                                List<TreeNode> nodes, ExpressionTree expressionTree){
        System.out.println("Join two tables: " + table1 +" and " + table2);
        SchemaManager mngr = Phi.schemaManager;
        Relation relation1 = mngr.getRelation(table1);
        Relation relation2 = mngr.getRelation(table2);
        Schema schema3 = joinTwoSchema(table1, table2, relation1.getSchema(), relation2.getSchema());
        // ie: "course_cross_course2"
        String table3 = table1 + "_cross_" + table2;
        // create an empty relation
        mngr.createRelation(table3, schema3);
        Relation relation3 = mngr.getRelation(table3);
        // create a new tuple
        Tuple tuple3 = relation3.createTuple();
        // read tuples from table1 and table2, join them then insert it into table3
        for(int i = 0; i < relation1.getNumOfBlocks(); i++){
            // read block[i] from relation1 and copy it into main memory
            relation1.getBlock(i, 0);
            // read it
            Block block1 = Phi.mainMemory.getBlock(0);
            if(block1.getNumTuples() == 0) continue;
            for(Tuple tuple1:block1.getTuples()){
                // selection pushed down
                if(!check(table1, tuple1, nodes, expressionTree)) continue;
                // inner loop for relation2
                for(int j = 0; j < relation2.getNumOfBlocks(); j++){
                    relation2.getBlock(j,2);
                    Block block2 = Phi.mainMemory.getBlock(2);
                    if(block2.getNumTuples() == 0) continue;
                    for(Tuple tuple2:block2.getTuples()){
                        if(!check(table2, tuple2, nodes, expressionTree)) continue;
                        tuple3 = joinTwoTuples(tuple1,tuple2,tuple3);
                        Phi.appendTuple(relation3, Phi.mainMemory, 9, tuple3);
                    }
                }
            }
        }
        return table3;
    }

    /** Join two tables **/
    public String joinTwoTables(Main Phi, String table1, String table2){
        System.out.println("Join two tables: " + table1 +" and " + table2);
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

    /** Join tables with where condition**/

    public List<String> joinTables(Main Phi, List<String> tables, ExpressionTree expressionTree){
        TreeNode root = expressionTree.getRoot();
        // sub conditions
        List<TreeNode> nodes = splitNode(root);
        String table1, table2, table3;
        List<String> tempTables = new ArrayList<>();
        int index = 0;
        for(; index < tables.size(); index ++){
            if(index == 0){
                table1 = tables.get(index);
                table2 = tables.get(index + 1);
                table3 = joinTwoTables(Phi, table1, table2, nodes, expressionTree);
                tempTables.add(table3);
                index ++;
            }else{
                table1 = tempTables.get(tempTables.size()-1);
                table2 = tables.get(index);
                table3 =joinTwoTables(Phi, table1, table2, nodes, expressionTree);
                tempTables.add(table3);
            }
        }
        return tempTables;
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

    /** Helper function: split nodes **/
    private List<TreeNode> splitNode(TreeNode node){
        List<TreeNode> nodes = new ArrayList<>();
        if(!"&|".contains(node.getValue())){
            nodes.add(node);
        }else{
            nodes.addAll(splitNode(node.left));
            nodes.addAll(splitNode(node.right));
        }
        return nodes;
    }



    public void DropTables(Main Phi, List<String> tables){
        for(int i=0; i < tables.size(); i++){
            SchemaManager mngr = Phi.schemaManager;
            mngr.deleteRelation(tables.get(i));
        }
    }

    private boolean check(String table, Tuple tuple, List<TreeNode> nodes, ExpressionTree expressionTree){
        // joined table, already checked
        if(table.contains("_cross_")) return true;
        Set<String> tableSet = new HashSet<>();
        for(TreeNode node:nodes){
            tableSet = pushToWhich(node);
            if(tableSet.size() > 1) continue;
            if(tableSet.size() == 1){
                if(!tableSet.iterator().next().equals(table)) continue;
                return expressionTree.check(tuple, rmTableName(node));
            }
        }
        return true;
    }
    /** Handle case: course.sid in sub node but only 'sid' in single table **/
    private TreeNode rmTableName(TreeNode node){
        // deep copy node
        TreeNode newNode = node.deepCopy();
        if(newNode.left == null || newNode.right == null) {
            String curVal = newNode.getValue();
            if(curVal.contains(".")){
                newNode.setValue(curVal.split("\\.")[1]);
                return newNode;
            }
        }
        // DFS from top to down
        newNode.left = rmTableName(newNode.left);
        newNode.right = rmTableName(newNode.right);
        return newNode;
    }

    private Set<String> pushToWhich(TreeNode node){
        Set<String> tableSet = new HashSet<>();
        if(node.left == null || node.right == null){
            // handle case: course.id
            if(node.value.contains(".")){
                tableSet.add(node.getValue().split(".")[0]);
                return tableSet;
            }
        }
        tableSet.addAll(pushToWhich(node.left));
        tableSet.addAll(pushToWhich(node.right));
        return tableSet;
    }

    /** Whether input str matches digits regex **/
    private boolean isInteger(String str){
        return str.matches("\\d+");
    }
}
