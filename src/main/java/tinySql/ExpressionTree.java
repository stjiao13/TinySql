package main.java.tinySql;

import java.util.Stack;

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

    public String toString(){
        return value;
    }
}

public class ExpressionTree {
    public TreeNode root;
    private Stack<String> operator;
    private Stack<TreeNode> operand;

    public ExpressionTree(){}

    // construct tree
    public TreeNode buildTree(String str){
        // TODO
        TreeNode root = new TreeNode();
        // construct expression tree according to grammars, +,-,*,/
        return root;
    }

    public static void main(String[] args){
        
    }
}
