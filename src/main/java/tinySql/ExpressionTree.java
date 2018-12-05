package main.java.tinySql;

import main.java.storageManager.Field;
import main.java.storageManager.Schema;
import main.java.storageManager.Tuple;

import java.util.*;

class TreeNode{
    /*
    TreeNode class
    * */
    String value;
    TreeNode left;
    TreeNode right;

    public TreeNode(){ }

    public TreeNode(String value){
        this.value = value;
    }

    public TreeNode(String value, TreeNode left, TreeNode right){
        this.value = value;
        this.left = left;
        this.right = right;
    }

    public String getValue(){
        return value;
    }

    public TreeNode getLeft(){
        return left;
    }

    public TreeNode getRight(){
        return right;
    }

    public void setValue(String value){
        this.value = value;
    }

    public TreeNode deepCopy(){
        TreeNode left = null;
        TreeNode right = null;
        if(this.left != null){
            left = this.left.deepCopy();
        }
        if(this.right != null){
            right = this.right.deepCopy();
        }
        return new TreeNode(value, left, right);
    }
}

public class ExpressionTree {
    public TreeNode root;
    private Stack<String> operator;
    private Stack<TreeNode> operand;

    // operator preference
    private static final Map<String, Integer > preference;
    static
    {
        preference = new HashMap<>();
        preference.put("/",4);
        preference.put("*",4);
        preference.put("+",3);
        preference.put("-",3);
        preference.put(">",3);
        preference.put("<",3);
        preference.put("=",2);
        preference.put("!",1);
        preference.put("&",0);
        preference.put("|",-1);
        preference.put("(",-2);
        preference.put(")",-2);
    }


    public ExpressionTree(String str){
        /* Constructor */
        this.operator = new Stack<>();
        this.operand = new Stack<>();
        this.root = buildTree(str);
    }

    public ExpressionTree(){
        /* Default Constructor */
        this.operator = new Stack<>();
        this.operand = new Stack<>();
        this.root = new TreeNode();
    }

    public TreeNode getRoot(){
        return root;
    }

    public void setRoot(TreeNode root){
        this.root = root;
    }

    public TreeNode buildTree(String str){
        /*
        construct expression tree according to grammars, +,-,*,/,(,),|,&,!
        * */
        if(str == null || str.length() == 0) return null;

        TreeNode root;

        List<String> words = split(str);
        // System.out.println("splited words: " + words);
        for(String part : words){

            if(isOperator(part)){
                if(part.equals("(")){
                    operator.push(part);
                }else if(part.equals(")")){
                    while((!operator.isEmpty()) && !operator.peek().equals("(")){
                        // connect tree nodes with higher preference
                        connect(operator.pop());
                    }
                    // remove "("
                    operator.pop();
                }else{
                    // System.out.println("non () operator part: " + part);
                    //process precedence
                    int pre = preference.get(part);
                    // System.out.println(operator);
                    while((!operator.isEmpty()) && (pre)<=preference.get(operator.peek())){
                        // connect tree nodes with higher preference
                        connect(operator.pop());
                    }
                    // don't forget to push it into operator stack!
                    operator.push(part);
                }
            }else{
                // System.out.println("operand part: " + part);
                operand.push(new TreeNode(part));
            }
        }
        // System.out.println("operator stack: " + operator);
        while(!operator.isEmpty()){
            connect(operator.pop());
        }
        // the peek node is the root
        root = operand.pop();
        return root;
    }


    private void connect(String oprt){
        /* helper function: connect children nodes to parent node */
        TreeNode right = operand.pop();
        TreeNode left = null;
        TreeNode node;
        if(oprt.equals("!")){
            node = new TreeNode("!",new TreeNode("false"), right);
        }else{
            left = operand.pop();
            node = new TreeNode(oprt, left, right);
        }
        operand.push(node);
    }


    public List<String> split(String str){
        /*
        helper function: split statement into a list of words
        ie: "course.sid = course2.sid AND course.exam > course2.exam"
            [course.sid, =, course2.sid, &&, course.exam, >, course2.exam]
        * */
        List<String> words = new ArrayList<>();

        int index = 0;

        for(; index < str.length(); index ++){
            // trim spaces
            while(index < str.length() && Character.isSpaceChar(str.charAt(index))){
                index ++;
            }
            if(Character.isLetterOrDigit(str.charAt(index)) || str.charAt(index) == '.'){
                StringBuilder sb = new StringBuilder();
                // letters or digit or "a.b"
                while(index < str.length() &&
                    (Character.isLetterOrDigit(str.charAt(index)) ||
                    str.charAt(index) == '.')){

                    sb.append(str.charAt(index));
                    index ++;
                }
                index --;
                String s = sb.toString();
                if(s.toLowerCase().equals("and")) words.add("&");
                else if(s.toLowerCase().equals("or")) words.add("|");
                else if(s.toLowerCase().equals("not")) words.add("!");
                else words.add(s);
            }else{
                // operators: + - * / = ( ) [ ]
                char c = str.charAt(index);
                if(c == '[') c = '(';
                if(c ==']') c = ')';
                // System.out.println("c: " + c);
                words.add(String.valueOf(c));
            }
        }

        return words;
    }

    private boolean isOperator(String s){
        /*
        helper function: char is operator or not
        * */
        char c = s.charAt(0);
        if( c == '>' ||
            c == '<' ||
            c == '=' ||
            c == '+' ||
            c == '-' ||
            c == '*' ||
            c == '/' ||
            c == '&' ||
            c == '|' ||
            c == '!' ||
            c == '(' ||
            c == ')'){
            return true;
        }
        return false;
    }

    public boolean check(Tuple tuple, TreeNode node){
        return Boolean.parseBoolean(evaluate(tuple, node));
    }

    private String getTuple(Tuple tuple, String expression){
        /*
        get corresponding tuple filed value/attribute according to sub expression
        take care of "." situation: "course.sid"
        * */
        List<String> filedNames = tuple.getSchema().getFieldNames();
        int dotLoc = expression.indexOf(".");
        for(int i = 0; i < filedNames.size(); i++){
            // value
            if(filedNames.get(i).equalsIgnoreCase(expression)){
                // Field f = tuple.getField(i);
                return tuple.getField(expression).toString().replaceAll("\\\"", "");
            }
            if(dotLoc!=-1 && filedNames.get(i).equalsIgnoreCase(expression.substring(dotLoc+1))){
                return tuple.getField(expression.substring(dotLoc+1)).toString().replaceAll("\\\"", "");
            }
        }
        expression = expression.replaceAll("\\\"", "");
        // attribute
        //System.out.println("Expression: " + expression);
        return expression;
    }

    /** Whether input str matches digits regex **/
    private boolean isInteger(String str){
        return str.matches("\\d+");
    }

    /**
        Evaluate input tuple whether satisfies the expression
        (expression tree rooted at input node)
     * **/
    public String evaluate(Tuple tuple, TreeNode node){

        if(node == null) return null;

        String curOp = node.getValue();
        String leftOp, rightOp;

        leftOp = evaluate(tuple, node.getLeft());
        rightOp = evaluate(tuple, node.getRight());

        if(curOp.equals("=")){
            if(isInteger(leftOp)) {
                // are digits, compare values
                return String.valueOf(Integer.parseInt(leftOp) == Integer.parseInt(rightOp));
            } else {
                // are strings, compare strings
                return String.valueOf(leftOp.equalsIgnoreCase(rightOp));
            }
        }
        else if(curOp.equals(">")){
            return String.valueOf(Integer.parseInt(leftOp) > Integer.parseInt(rightOp));
        }
        else if(curOp.equals("<")){
            return String.valueOf(Integer.parseInt(leftOp) < Integer.parseInt(rightOp));
        }
        else if(curOp.equals("+")){
            return String.valueOf(Integer.parseInt(leftOp) + Integer.parseInt(rightOp));
        }
        else if(curOp.equals("-")){
            return String.valueOf(Integer.parseInt(leftOp) - Integer.parseInt(rightOp));
        }
        else if(curOp.equals("*")){
            return String.valueOf(Integer.parseInt(leftOp) * Integer.parseInt(rightOp));
        }
        else if(curOp.equals("/")){
            return String.valueOf(Integer.parseInt(leftOp) / Integer.parseInt(rightOp));
        }
        else if(curOp.equals("&")){
            return String.valueOf(Boolean.parseBoolean(leftOp) && Boolean.parseBoolean(rightOp));
        }
        else if(curOp.equals("|")){
            return String.valueOf(Boolean.parseBoolean(leftOp) || Boolean.parseBoolean(rightOp));
        }
        else if(curOp.equals("!")){
            // return (Boolean.parseBoolean(rightOp) == true ? "false" : "true");
            return String.valueOf(!Boolean.parseBoolean(rightOp));
        }
        else{
            if(isInteger(curOp)){
                return curOp;
            }else {
                return getTuple(tuple, curOp);
            }
        }
    }

    public String toString(TreeNode node){
        /*
        Inorder print the tree which rooted at this node
        * */
        if(node == null) return "";
        String str = toString(node.left) + node.value + toString(node.right);
        return str;
    }

    public static void main(String[] args){
        String stmt = "( exam = 100 or homework = 100 )";
        ExpressionTree test = new ExpressionTree();
        TreeNode root = test.buildTree(stmt);
        System.out.println(root.getValue());
        System.out.println(root.left.getValue());
        System.out.println(root.right.getValue());
        System.out.println(test.toString(root));
    }
}
