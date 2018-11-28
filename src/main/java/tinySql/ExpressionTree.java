package main.java.tinySql;

import java.util.*;

class TreeNode{
    public String value;
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

    // print tree rooted at this node
    public String toString(TreeNode node){
        if(node == null) return "";
        String str = toString(node.left) + node.value + toString(node.right);
        return str;
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
        preference.put("(",0);
        preference.put(")",0);
        preference.put("|",-1);
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

    public TreeNode buildTree(String str){
        /*
        construct expression tree according to grammars, +,-,*,/,(,)
        * */
        if(str == null || str.length() == 0) return null;

        TreeNode root;

        List<String> words = split(str);
        for(String part : words){
            if(isOperator(part)){
                if(part.equals("(")){
                    operator.push(part);
                    break;
                }else if(part.equals(")")){
                    while((!operator.isEmpty()) && !operator.peek().equals("(")){
                        // connect tree nodes with higher preference
                        connect(operator.pop());
                    }
                    // remove "("
                    operator.pop();
                }else{
                    int pre = preference.get(part);
                    while((!operator.isEmpty()) && (pre)<=preference.get(operator.peek())){
                        // connect tree nodes with higher preference
                        connect(operator.pop());
                    }
                    // don't forget to push it into operator stack!
                    operator.push(part);
                }
            }else{
                operand.push(new TreeNode(part));
            }
        }
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
        TreeNode left;
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
                System.out.println("space char: " + str.charAt(index));
                index ++;
            }
            System.out.println("char: " + str.charAt(index));
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
                System.out.println("word: " + s);
                if(s.toLowerCase().equals("and")) words.add("&&");
                else if(s.toLowerCase().equals("or")) words.add("||");
                else if(s.toLowerCase().equals("not")) words.add("!");
                else words.add(s);
            }else{
                // operators: + - * / = ( )
                System.out.println("op: " + str.charAt(index));
                words.add(String.valueOf(str.charAt(index)));
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
            c == '!' ){
            return true;
        }
        return false;
    }

    public static void main(String[] args){
        String stmt = "course.sid = course2.sid AND course.exam > course2.exam";
        ExpressionTree test = new ExpressionTree();
        System.out.println(test.split(stmt));
    }
}
