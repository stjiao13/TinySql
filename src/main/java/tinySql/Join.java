package main.java.tinySql;

import main.java.storageManager.*;

import java.util.*;

class TupleWithBlockId{
    public Tuple tuple;
    public int blockId;
    public TupleWithBlockId(Tuple tuple, int blockId){
        this.tuple = tuple;
        this.blockId = blockId;
    }
}

class RelationIterator {
    private MainMemory mainMemory;
    private Relation relation;
    private int memTmpBlockId;
    private int curBlockId;
    private Queue<Tuple> tupleQueue;
    private int numBlocks;
    public RelationIterator(Relation relation, MainMemory mainMemory, int memTmpBlockId) {
        this.relation = relation;
        this.mainMemory = mainMemory;
        this.memTmpBlockId = memTmpBlockId;
        this.curBlockId = -1;
        this.tupleQueue = new LinkedList<>();
        this.numBlocks = relation.getNumOfBlocks();
    }

    public boolean hasNext() {
        if (!tupleQueue.isEmpty()) {
            return true;
        } else {
            while (tupleQueue.isEmpty()) {
                curBlockId++;
                if (curBlockId >= numBlocks) {
                    return false;
                }
                mainMemory.getBlock(memTmpBlockId).clear();
                relation.getBlock(curBlockId, memTmpBlockId);
                if (!mainMemory.getBlock(memTmpBlockId).isEmpty()) {
                    for (Tuple tuple : mainMemory.getBlock(memTmpBlockId).getTuples()) {
                        if (!tuple.isNull()) {
                            tupleQueue.offer(tuple);
                        }
                    }
                }
            }
            return true;
        }
    }

    public Tuple next() {
        if (!hasNext()) {
            return null;
        }
        return tupleQueue.poll();
    }
}

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
        // index -= tuple1.getNumOfFields();
        index = 0;
        for(; index < tuple2.getNumOfFields(); index++) {
            String value = tuple2.getField(index).toString();
            //System.out.println("offset: " + index + tuple1.getNumOfFields());
            if(isInteger(value)) { tuple3.setField(index + tuple1.getNumOfFields(), Integer.parseInt(value)); }
            else { tuple3.setField(index + tuple1.getNumOfFields(), value); }
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
        //System.out.println("new filed names: " + fieldNames);
        //System.out.println("new filed types: " + fieldTpyes);
        Schema schema3 = new Schema(fieldNames, fieldTpyes);
        //System.out.println("new schema's number of fields: " + schema3.getNumOfFields());
        return schema3;
    }

    /** Join two tables with where condition **/
    public String joinTwoTables(Main Phi, String table1, String table2,
                                List<TreeNode> nodes, ExpressionTree expressionTree){
        // System.out.println("Join two tables: " + table1 +" and " + table2);
        SchemaManager mngr = Phi.schemaManager;
        Relation relation1 = mngr.getRelation(table1);
        Relation relation2 = mngr.getRelation(table2);
        Schema schema3 = joinTwoSchema(table1, table2, relation1.getSchema(), relation2.getSchema());
        //System.out.println("new schema's fields: " + schema3.getNumOfFields());
        // ie: "course_cross_course2"
        String table3 = table1 + "_cross_" + table2;
        // create an empty relation
        mngr.createRelation(table3, schema3);
        Relation relation3 = mngr.getRelation(table3);
        //System.out.println("new relation: \n" + relation3);
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
        // System.out.println("Join two tables: " + table1 +" and " + table2);
        SchemaManager mngr = Phi.schemaManager;
        //System.out.println("cur manager has course2: " + mngr.relationExists("course2"));
        Relation relation1 = mngr.getRelation(table1);
        Relation relation2 = mngr.getRelation(table2);
        // join two schema
        Schema schema3 = joinTwoSchema(table1, table2, relation1.getSchema(), relation2.getSchema());
        // System.out.println("new schema's fields: " + schema3.getNumOfFields());
        // ie: "course_cross_course2"
        String table3 = table1 + "_cross_" + table2;
        // a new empty relation
        mngr.createRelation(table3, schema3);
        Relation relation3 = mngr.getRelation(table3);
        //System.out.println("new relation: " + relation3.getRelationName());
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
                for(int j = 0; j < relation2.getNumOfBlocks(); j++){
                    relation2.getBlock(j,2);
                    Block block2 = Phi.mainMemory.getBlock(2);
                    if(block2.getNumTuples() == 0) continue;
                    for(Tuple tuple2 : block2.getTuples()){
                        tuple3 = joinTwoTuples(tuple1, tuple2, tuple3);
                        //System.out.println("tuple 3" + tuple3);
                        Phi.appendTuple(relation3, Phi.mainMemory, 5, tuple3);
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
            // System.out.println("sub node: " + expressionTree.toString(node));
            tableSet = pushToWhich(node);
            // System.out.println("table set: " + tableSet);
            if(tableSet.size() > 1) continue;
            if(tableSet.size() == 1){
                if(!tableSet.iterator().next().equals(table)) continue;
                // System.out.println("tuple: " + tuple);
                TreeNode newNode = rmTableName(node);
                // System.out.println("new node: " + expressionTree.toString(newNode));
                //return expressionTree.check(tuple, rmTableName(node));
                return expressionTree.check(tuple, newNode);
            }
        }
        return true;
    }
    /** Handle case: course.sid in sub node but only 'sid' in single table **/
    private TreeNode rmTableName(TreeNode node){
        if(node == null) return null;
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
                String val = node.getValue();
                tableSet.add(node.getValue().split("\\.")[0]);
                return tableSet;
            }
        }
        if(node.left != null){
            tableSet.addAll(pushToWhich(node.left));
        }
        if(node.right != null){
            tableSet.addAll(pushToWhich(node.right));
        }
        return tableSet;
    }

    /** Whether input str matches digits regex **/
    private boolean isInteger(String str){
        return str.matches("\\d+");
    }

    public String naturalJoinTwoTables(Main Phi, String table1, String table2, String condition){
        Phi.clearMainMemory();
        SchemaManager mngr = Phi.schemaManager;
        Relation relation1 = mngr.getRelation(table1);
        Relation relation2 = mngr.getRelation(table2);
        Schema schema1 = mngr.getSchema(table1);
        Schema schema2 = mngr.getSchema(table2);
        String key1 = condition.split("=")[0].trim();
        if (!schema1.getFieldNames().contains(key1) && key1.indexOf(".") != -1) {
            key1 = key1.split("\\.")[1];
        }
        String key2 = condition.split("=")[1].trim();
        if (!schema2.getFieldNames().contains(key2) && key2.indexOf(".") != -1) {
            key2 = key2.split("\\.")[1];
        }

        String tempRelationName = table1+"_natureJoin_"+table2;
        Schema tempSchema = joinTwoSchema(table1,table2,schema1,schema2);
        Relation tempRelation = Phi.schemaManager.createRelation(tempRelationName,tempSchema);

        List<String> sortFieldsOne = new ArrayList<>();
        List<String> sortFieldsTwo = new ArrayList<>();
        sortFieldsOne.add(key1);
        sortFieldsTwo.add(key2);
        twoPassSort(Phi, relation1, sortFieldsOne);
        twoPassSort(Phi, relation2, sortFieldsTwo);
        RelationIterator relationIterator1 = new RelationIterator(relation1, Phi.mainMemory, 0);
        RelationIterator relationIterator2 = new RelationIterator(relation2, Phi.mainMemory, 1);
        Tuple tuple1 = null;
        Tuple tuple2 = null;
        // two pointers
        do{
            if(tuple1 == null && tuple2 == null){
                tuple1 = relationIterator1.next();
                tuple2 = relationIterator2.next();
            }
            int compare = compareTuplesByKey(tuple1, tuple2, key1, key2);
            if(compare < 0){
                tuple1 = relationIterator1.next();
            }else if(compare > 0){
                tuple2 = relationIterator2.next();
            }else{
                List<Tuple> sameTupleListOne = new ArrayList<>();
                List<Tuple> sameTupleListTwo = new ArrayList<>();
                Tuple preTupleOne = tuple1;
                Tuple preTupleTwo = tuple2;
                sameTupleListOne.add(preTupleOne);
                sameTupleListTwo.add(preTupleTwo);
                while (tuple1 != null) {
                    tuple1 = relationIterator1.next();
                    if (tuple1 == null) {
                        break;
                    }
                    if (compareTuplesByKey(preTupleOne, tuple1, key1, key1) == 0) {
                        sameTupleListOne.add(tuple1);
                    } else {
                        break;
                    }
                }

                while (true) {
                    tuple2 = relationIterator2.next();
                    if (tuple2 == null) {
                        break;
                    }
                    if (compareTuplesByKey(preTupleTwo, tuple2, key2, key2) == 0) {
                        sameTupleListTwo.add(tuple2);
                    } else {
                        break;
                    }
                }
                for (Tuple sameTupleOne : sameTupleListOne) {
                    for (Tuple sameTupleTwo : sameTupleListTwo) {
                        Tuple joinedTuple = tempRelation.createTuple();
                        joinedTuple = joinTwoTuples(sameTupleOne, sameTupleTwo, joinedTuple);
                        if (joinedTuple == null) {
                            continue;
                        }
                        Phi.appendTuple(tempRelation, Phi.mainMemory, Phi.mainMemory.getMemorySize() - 1, joinedTuple);
                    }
                }
            }
        }while(tuple1!= null && tuple2!=null);
        return tempRelationName;
    }

    public int compareTuplesByKey(Tuple o1, Tuple o2, String key1, String key2){
        Field field1 = o1.getField(key1);
        Field field2 = o2.getField(key2);
        if(field1.type == FieldType.INT){
            return field1.integer - field2.integer;
        }else{
            return field1.str.compareTo(field2.str);
        }
    }

    private void twoPassSort(Main Phi, Relation relation, List<String> sortFields){
        int numRealBlocks = relation.getNumOfBlocks();
        int numMemBlocks = Phi.mainMemory.getMemorySize();
        int numSublists = (numRealBlocks % numMemBlocks == 0) ? numRealBlocks / numMemBlocks
                : numRealBlocks / numMemBlocks + 1;
        for(int i = 0; i < numSublists; i++){
            Phi.clearMainMemory();
            // 这个内部循环似乎没用
            for (int j = 0; j < numMemBlocks; j++) {
                int offset = i * numMemBlocks + j;
                if (offset >= numRealBlocks) {
                    break;
                }
                relation.getBlock(offset, j);
            }
            // sort this sublist 先用collections
            Comparator<Tuple> comp = new Comparator<Tuple>() {
                @Override
                public int compare(Tuple o1, Tuple o2) {
                    for (String sortField:sortFields){
                        Field field1 = o1.getField(sortField);
                        Field field2 = o2.getField(sortField);
                        if (field1.type == FieldType.INT) {
                            if (field1.integer != field2.integer) {
                                return ((Integer) field1.integer).compareTo(field2.integer);
                            }
                        } else if (field1.type == FieldType.STR20) {
                            if (!field1.str.equals(field2.str)) {
                                return field1.str.compareTo(field2.str);
                            }
                        }
                    }
                    return 0;
                }
            };

            // offer main memory tuples into heap
            // 此时要保证main memory 所有的tuples都insert到了heap里面
            // hacky 的方法是直接把tuple放到List里面然后collection sort
            Heap heap = new Heap(10000, comp);
            int count = 0;
            for (int k = 0; k < numMemBlocks ; k++) {
                Block block = Phi.mainMemory.getBlock(k);
                if (!block.isEmpty() && block.getNumTuples() > 0) {
                    for (Tuple tuple : block.getTuples()) {
                        if (tuple.isNull()) {
                            continue;
                        }
                        count ++;
                        heap.insert(new HeapNode(count, tuple));
                    }
                }
            }
            Phi.clearMainMemory();
            System.out.println("Begin Heap Sort");
            //把heap 里面的tuple按照Order存回main memory
            int memBlockIndex = 0;
            while(!heap.isEmpty()){
                if(Phi.mainMemory.getBlock(memBlockIndex).isFull()){
                    memBlockIndex++;
                }
                Block curMemBlock = Phi.mainMemory.getBlock(memBlockIndex);
                // heap node's data is object, need to be transferred to tuple
                curMemBlock.appendTuple((Tuple) heap.poll().data);
            }

            // put blocks back into disk
            for(int j = 0; j < numMemBlocks; j++){
                int offset = numRealBlocks + i*numMemBlocks + j;
                if(offset >= numRealBlocks*2) break;
                relation.setBlock(offset, j);
            }
        }
        System.out.println("End sorting sublists. ");
        System.out.println("Begin merging sorted sublists");
        // merge sorted sublists
        int[] numTuplesRemainInBlock = new int[numSublists];
        Comparator<TupleWithBlockId> tuplewithBlockIdComp = new Comparator<TupleWithBlockId>() {
            @Override
            public int compare(TupleWithBlockId o1, TupleWithBlockId o2) {
                for (String sortField : sortFields) {
                    Field field1 = o1.tuple.getField(sortField);
                    Field field2 = o2.tuple.getField(sortField);
                    if (field1.type == FieldType.INT) {
                        if (field1.integer != field2.integer) {
                            return ((Integer) field1.integer).compareTo(field2.integer);
                        }
                    } else if (field1.type == FieldType.STR20) {
                        if (!field1.str.equals(field2.str)) {
                            return field1.str.compareTo(field2.str);
                        }
                    }
                }
                return 0;
            }
        };
        Heap tupleWithBlockIdHeap = new Heap(10000, tuplewithBlockIdComp);
        int nodecounts = 0;
        // add tuples of the first block into heap
        for(int i = 0; i < numSublists; i++){
            int offset = numRealBlocks + i*numMemBlocks;
            Phi.mainMemory.getBlock(0).clear();
            relation.getBlock(offset, 0);
            Block curMemBlock = Phi.mainMemory.getBlock(0);
            numTuplesRemainInBlock[i] = curMemBlock.getNumTuples();
            if(curMemBlock.getNumTuples() <= 0) continue;
            for(Tuple tuple : curMemBlock.getTuples()){
                if(tuple.isNull()) continue;
                TupleWithBlockId updateTuple = new TupleWithBlockId(tuple, offset);
                tupleWithBlockIdHeap.insert(new HeapNode(nodecounts, updateTuple));
                nodecounts ++;
            }
        }
        // use heap to fill memory
        Phi.clearMainMemory();
        int curMemBlockId = 0;
        int curDiskBlockId = 0;
        int storeMemBlockId = 1;
        boolean isLastFull = false;

        while(!tupleWithBlockIdHeap.isEmpty()){
            TupleWithBlockId curTupleWithBlockId = (TupleWithBlockId) tupleWithBlockIdHeap.poll().data;
            int blockId = curTupleWithBlockId.blockId;
            int sublistId = (blockId - numRealBlocks)/numMemBlocks;
            numTuplesRemainInBlock[sublistId] --;
            // if we have polled all tuples of a bloc, we add the next block
            // in the sublist into heap if there is a one
            if(numTuplesRemainInBlock[sublistId] == 0){
                if ((blockId - numRealBlocks) % numMemBlocks != numMemBlocks - 1 && blockId + 1 < numRealBlocks * 2) {
                    Phi.mainMemory.getBlock(curMemBlockId).clear();
                    relation.getBlock(blockId + 1, curMemBlockId);
                    // not empty
                    Block curMemBlock = Phi.mainMemory.getBlock(curMemBlockId);
                    if (!curMemBlock.isEmpty() && curMemBlock.getNumTuples() > 0) {
                        numTuplesRemainInBlock[sublistId] = curMemBlock.getNumTuples();
                        for (Tuple tuple : curMemBlock.getTuples()) {
                            if (tuple.isNull()) {
                                numTuplesRemainInBlock[sublistId]--;
                                continue;
                            }
                            TupleWithBlockId newTuple = new TupleWithBlockId(tuple, blockId + 1);
                            tupleWithBlockIdHeap.insert(new HeapNode(nodecounts,newTuple));
                            nodecounts ++;
                        }
                    }
                }
            }
            // use polled tuple fill a mem block and put it back to disk
            Phi.mainMemory.getBlock(storeMemBlockId).appendTuple(curTupleWithBlockId.tuple);
            isLastFull = false;
            if (Phi.mainMemory.getBlock(storeMemBlockId).isFull()) {
                relation.setBlock(curDiskBlockId, storeMemBlockId);
                curDiskBlockId++;
                Phi.mainMemory.getBlock(storeMemBlockId).clear();
                // relation.setBlock(curDiskBlockId, storeMemBlockId);
                isLastFull = true;
            }
        }
        if(isLastFull){
            relation.deleteBlocks(curDiskBlockId);
        }else{
            relation.deleteBlocks(curDiskBlockId+1);
        }
    }

    public boolean isNaturalJoin(TreeNode expressionTreeNode){
        boolean twoTable = tableRelevantToTheCondition(expressionTreeNode).size() == 2;
        boolean operand = expressionTreeNode.getValue().equals("=");
        if(!twoTable || operand) return false;
        String leftKey = expressionTreeNode.getLeft().getValue().split("\\.")[1];
        String rightKey = expressionTreeNode.getRight().getValue().split("\\.")[1];
        return leftKey.equals(rightKey);
    }

    private List<String> tableRelevantToTheCondition(TreeNode expressionTreeNode){
        List<String> tables = new ArrayList<>();
        if(expressionTreeNode == null){
            return tables;
        }
        String valueCurNode = expressionTreeNode.getValue();
        String tableCurNode = getTableFromOperand(valueCurNode);
        if(tableCurNode != null){
            if(!tables.contains(tableCurNode)){
                tables.add(tableCurNode);
            }
        }
        tables.addAll(tableRelevantToTheCondition(expressionTreeNode.getLeft()));
        tables.addAll(tableRelevantToTheCondition(expressionTreeNode.getRight()));
        return tables;
    }

    private String getTableFromOperand(String str){
        if(str == null || str.length() == 0){
            return null;
        }
        if(str.indexOf(".") == -1 && isValidRelationName(str)){
            return str;
        }else if(str.indexOf(".") != -1){
            String tableName = str.split("\\.")[0];
            if(isValidRelationName(tableName)){
                return tableName;
            }
        }
        return null;
    }

    private boolean isValidRelationName(String str){
        return str.matches("^[a-zA-Z]+[\\w\\d]*");
    }

    private boolean isValidColumnName(String str){
        if(str.indexOf(".") != -1){
            String[] splitResults = str.split("\\.");
            if(splitResults.length > 2){
                return false;
            }else{
                String relationName = splitResults[0];
                String columnName = splitResults[1];
                return isValidRelationName(relationName) && isValidRelationName(columnName);
            }
        }else{
            return isValidRelationName(str);
        }
    }
}
