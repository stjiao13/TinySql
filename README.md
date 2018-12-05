---
title:  CSCE608 Project 2 TinySQL
team memeber: Yuelin Zhang | zyl960822-@tamu.edu
team  member: Shutong Jiao | stj13@tamu.edu
---

## Introduction


This project report will give brief explanation of the software architecture of the tinysql project which includes interface, parser, logical and physical query plan. Then it discuss the optimizations made in logical and physical query plan in details. At last, it shows execution results on given tests and custom tests.

---
## To start with

1. Decompress out.zip to current directory

2. Change current directory
```
cd out/production/tinysql
```

3. Run the interface class:
```
java main.java.tinySql.Interface
```

## Software Architecture

### Interface

The user would either type a single sql statement or perform sql query on a text file with sql statements. User may also specify the filename of outputfileThis user-friendly interface makes user easier to implement sql statement using this program.

Below is the snapshot of the interface of our program.

### Parser

The parser class accepts a tiny sql statement as input and then return a parse tree after processing. We have defined DropNode, CreateNode, InsertNode for drop, create and insert operation. For delete and select method, we use a single class ParseTreeNode to represent delete and select operation since they share many common attributes.

#### Select Operation

```java
public class ParseTreeNode {
// type = "DELECT" or "SELECT"
public String type;
// whether stmt contains "DISTINCT" public boolean distinct;
// whether stmt contains "FROM"
public boolean from;
// whether stmt contains "WHERE"
public boolean where;
// whether stmt contains "order by" public boolean hasOrder;
// order condition
public String order_by;
// public ExpressionTree search_condition; // use string to represent ExpressionTree public String search_condition;
// attributes (columns)
public List<String> attributes;
// tables
public List<String> tablelist;
public ParseTreeNode parent; public ParseTreeNode child;
```
For the parseSelect method, we parse the input statement according to its type. As a typical sql select method could have many optional arguments, we need to deal with them with care. We need to judge if it is a select distinct statement as well as storing the attributes, tables and the search condition indicated by where.


#### Delete Operation

For delete operation we will generate the same parse tree node in Figure 1. A delete operation may also have search condition and we will deal with a similar way like in select operation

#### Create Operation

For create operation, the corresponding parse tree node is defined as below:

```java
public class CreateNode {
public String table_name;
public List<String[]> attribute_type_list;
```

Here, the value in attribute_type_list is the pair of name and type represented as string. Because create statement has no optional arguments, the parsing step is rather simple.

#### Drop Operation

For drop operation, the corresponding parse tree node is defined as below:

```java
 public class DropNode { 
 String table_name;
```

Like create operation, drop operation is very simple and will just need to store the table_name.

#### Insert Operation

For insert operation, the corresponding parse tree node is defined as below:

```java
public class InsertNode {
public String table_name;
public List<String> attribute_list;
public List<String> value_list_without_select; public ParseTreeNode value_lst_with_select;
```

There are two types of insert statement. One is to insert plain values and another is to insert values coming from the result of a select statement. Therefore we store the value as list of string in the first case and we store the result of ParseTreeNode in the second case.

### Logical Query Plan

For select or delete query with “Where” clause like: SELECT * FROM course WHERE exam = 100 AND project = 100, we need to evaluate tuples with such conditions. An Expression Tree to represent conditions and provide tuple evaluation functionality. A Boolean value will be returned to check whether a tuple satisfy expression tree’s condition.
Expression Tree:

```java
public class ExpressionTree { 
      public TreeNode root;
      private Stack<String> operator; private Stack<TreeNode> operand;
          // operator preference
      private static final Map<String, Integer > preference; ....
```

Evaluate a tuple:

```java
public boolean check(Tuple tuple, TreeNode node){ 
     return Boolean.parseBoolean(evaluate(tuple, node));
}
public String evaluate(Tuple tuple, TreeNode node){ 
      /*
      Evaluate input tuple whether satisfies the expression (expression tree rooted at input node)
      * */
       if(node == null) return null;
               String curOp = node.getValue();
               String leftOp, rightOp;
               leftOp = evaluate(tuple, node.getLeft());
               rightOp = evaluate(tuple, node.getRight());
       if(curOp.equals("=")){ if(isInteger(leftOp)) {
                       // are digits, compare values
       return String.valueOf(Integer.parseInt(leftOp) == Integer.parseInt(rightOp));
       } else {
       // are strings, compare strings return
       String.valueOf(leftOp.equalsIgnoreCase(rightOp));
                   }
       } ..........
```

### Physical Query Plan

In the main class of tinysql, we realize physical query plan. The instance variables we need are as below:

```
public class Main { Parser parser;
    MainMemory mainMemory;
    Disk disk;
    SchemaManager schemaManager;
    Join join;
```

Mainmemory, disk and schemaManger are the classes provided by storagemanager for realizing physical query plan. Join class is specific for join operations and will be discussed later.
We need to handle two types of input – single sql statement and a text file containing sql statements.
Main object will call exec for single sql statement which is shown below.
We could see that based on the type of the statement, specific query method will be called and we discuss them one by one.

```java
public void exec(String stmt) { 
/*
Analyse query statement then do
create/drop/insert/delete/select action
 * */

       String action = stmt.trim().toLowerCase().split("\\s+")[0];
               //System.out.println("Action: " + action);
       if(action.equals("create")){ this.createQuery(stmt);
       }else if(action.equals("drop")){ this.dropQuery(stmt);
       }else if(action.equals("insert")){ this.insertQuery(stmt);
       }else if(action.equals("delete")){ this.deleteQuery(stmt);
       }else if(action.equals("select")){ this.selectQuery(stmt);
       }else {
       // throw exception
       } 
}
```

#### Create Query

First, the createQuery method will call the parser to parse the create statement. Then it will create a new schema for the statement. Then the createRelation method of schemaManager is called to initialize the schema.

#### Drop Query

Similarly, the parser is called to parser the drop statement. Then the deleteRelation method of schemaManager is called to drop the table.

#### Insert query

First parse insert query statement to get the tableName and fieldValues. Then we create a nuew tuple and set field values into that tuple. At last we append the new tuple into the relation.
#### Delete query

The delete statement is first parsed to get the tableName and searchCondition . Then we will find all the blocks for this table. If there is no searchCondition , we just simply call the invalidateTuple method to invalidate all tuples. If there is some searchCondition,we will call the check method in the ExpressionTree class, which will recursively check whether the input tuple satisfy the expression tree.

#### Select query
Select query is the most common statement as well as the most complicated one. Here we will just discuss in general how select statement is implemented. In the optimization section we will detailed explanation about it.
Generally, we will have two types of select statemen – select from one table and select from multiple table. We have created corresponding method for the two case. So at first parser will be called and depending on the type different method will be called as shown below.


```
private void selectQuery(String stmt){ 
    try{
        // update select parser note
        parser.parseSelect(stmt);
        List<String> tableList = parser.selectNode.getTablelist();
        if(tableList.size() == 1){ 
              //select one table
              selectQuery1();
              }
              else{
               //select multi-tables
               selectQuery2();
               } 
         }
    catch (Exception e){ 
        System.out.println("e= " + e);
    } 
}
```

First, if we just select form one table we will statement like "select \[distinct\] (attributes or *) from (one table) where []"and selectQuery1 method will be called . The parser will be called and decide if we have optional arguments like distinct. Similar to the case of delete statement, we deal with where condition by calling the check method in the ExpressionTree class. If we have “distinct” keyword we drop the duplicate by using HashSet class. At last, we will define a custom comparator for tuple if we need to sort the result.
Then, if we need to select from multiple table, selectQuery2 method will be called. We have implemented a helper class called Join to optimize the join operation. We will deal with natural join and cross join, which will all be handled by the join class. After the join operation, we actually convert multiple table into one and then we could call selectQuery1 method like mentioned above.


#### Distinct and Order by condition:

Apart from “Where” conditions, “distinct” and “Order by” operations are also realized in physical query plan.
So we create a UniqueTuple class, override hashcode and comparator. Based on this class, we realize one pass duplication elimination and sorting.

```java
public class uniqueTuple implements Comparable<uniqueTuple>{
    private List<Field> fields;
    private List<String> selectedFieldNames;
    private Field key;

    public int hashCode(){
        String str = "";
        for(Field f:fields){
            str += f;
        }
        return str.hashCode();
    }

    public int compareTo(uniqueTuple tuple2){
        if(key.type == FieldType.STR20){
            return key.str.compareTo(tuple2.key.str);
        }else{
      return ((Integer)key.integer).compareTo(tuple2.key.integer);
        }
    }
...
```

#### Join Operation:

##### Cross join:

Join tables couple by couple. A nested loop is used.

```
/** Cross Join tables with where condition**/
public List<String> joinTables(Main Phi, List<String> tables, ExpressionTree expressionTree){
TreeNode root = expressionTree.getRoot(); // sub conditions
List<TreeNode> nodes = splitNode(root); String table1, table2, table3;
List<String> tempTables = new ArrayList<>();
...
```

##### Natural join:

In this case, we apply two pass sort algorithm. The basic idea for the two pass algorithm is to first make sorted sublists and then merge the sorted sublists.
To begin with, we need to define a heap class that allow us extract the minimum number efficiently. Our heap is implemented based binary heap and the basic structure of heap class is shown below:

```
public class Heap{
public boolean isEmpty(){
return lastIndex == 0; }
public HeapNode peek(){ ...
}
public HeapNode poll(){ ...
}
public void insert(HeapNode node){ ...
}
public boolean delete(HeapNode node){ ...
}
public void swimUp(int pos){ ...
}
public void sinkDown(int pos){ }
```
Then we need to implement the twoPassSort method which sort all tuples in a relation base a list of sort fields. For this method, we first sort sublists with collections. Then we construct a heap with all main memory tuples. After that we push back all tuples in the heap to memory and then write to disk. We keep doing so for the remaining sublists and finished the first step. For the second step, we make a class called TupleWithBlockId and construct a heap based on it. Last, we poll the tuple from the heap to the memory and then write back to disk.
We also define a RelationIterator class within the join class. The purpose of the iterator is to act as two pointers for the sorted blocks of the two tables. When we are comparing tuples, we could move the pointer right if any of the blocks is exhauseted.
Lastly a naturalJoinTwoTables method is implemented within the join class. This method will first two pass sort the two relations. Then use two RelationIterators to find the natural join result.

```
public class Join {
public boolean isNaturalJoin(TreeNode expressionTreeNode){
...
private void twoPassSort(Main Phi, Relation relation, List<String> sortFields){
int numRealBlocks = relation.getNumOfBlocks();
int numMemBlocks = Phi.mainMemory.getMemorySize(); ...
       // initialize a heap
Heap heap = new Heap(10000, comp); int count = 0;
for (int k = 0; k < numMemBlocks ; k++) {
Block block = Phi.mainMemory.getBlock(k);
if (!block.isEmpty() && block.getNumTuples() > 0) {
} }
for (Tuple tuple : block.getTuples()) { if (tuple.isNull()) {
continue; }
count ++;
heap.insert(new HeapNode(count, tuple)); }
            Phi.clearMainMemory();
            // Begin Heap Sort
            ......
           // merge sorted sublists
int[] numTuplesRemainInBlock = new int[numSublists];
Comparator<TupleWithBlockId> tuplewithBlockIdComp = new Comparator<TupleWithBlockId>() {
@Override
....
```

## Optimization

We have two major optimizations in this project. The first is to optimize the select operation in the expression TreeNode. Another is to optimize the natural join based two-pass sort algorithm.

### Optimize select operation
For select operation, a practical tragedy to split nodes whenever possible so that we could apply select operation early thus reduce the number of tuples. In join class, we have a method splitNode to split nodes if possible as shown below:

```
/** Helper function: split nodes **/
private List<TreeNode> splitNode(TreeNode node){ List<TreeNode> nodes = new ArrayList<>(); if(!"&|".contains(node.getValue())){
nodes.add(node); }else{
        nodes.addAll(splitNode(node.left));
        nodes.addAll(splitNode(node.right));
    }
return nodes; }
```

Then this method will be called when we are going to join tables as shown below:

```
public List<String> joinTables(Main Phi, List<String> tables, ExpressionTree expressionTree){
TreeNode root = expressionTree.getRoot(); // sub conditions
List<TreeNode> nodes = splitNode(root); String table1, table2, table3;
List<String> tempTables = new ArrayList<>(); int index = 0;
for(; index < tables.size(); index ++){ ......
```

## Experiment and results

### Experiment on given test
We first perform test by testStorageManger and the result of the experiment on the given test as below:

```
Computer elapse time = 3244 ms
Calculated elapse time = 3156.1600000000017 ms 
Calculated Disk I/Os = 44
```

We also perform test on the text.txt file given by the professor. The time used is 416s. And the complete output is attached at last.

### Experiment on custom test

First, we test on the impact of number of tuples on disk IO and running time. So we keep the query fixed and increase the size of input by simply duplicating it.

```
CREATE TABLE course (sid INT, homework INT, project INT, exam INT, grade STR20)
INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 99, 100, 100, "A")
INSERT INTO course (sid, homework, project, exam, grade) VALUES (3, 100, 100, 98, "C")
INSERT INTO course (sid, homework, project, exam, grade) VALUES (3, 100, 69, 64, "C")
.......
```

The sql statements below are adapted from the given test. And we test the disk IO / running time from 12 to 200.

