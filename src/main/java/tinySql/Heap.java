package main.java.tinySql;

import main.java.storageManager.Field;
import main.java.storageManager.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

class HeapNode<T>{
    public int id;
    public T data;
    public HeapNode(int id, T data){
        this.id = id;
        this.data = data;
    }
}

/*
    A binary heap is a heap data structure created using a binary tree.
    1. the tree is a complete binary tree
    2. each node is greater/smaller(depends on comparator) to each of its children.
* */
public class Heap{
    private int lastIndex;
    private int[] posArray;
    private HeapNode[] dataArray;
    private Comparator comparator;

    public Heap(int size, Comparator comparator){
        lastIndex = 0;
        this.comparator = comparator;
        posArray = new int[size];
        Arrays.fill(posArray, -1);
        dataArray = new HeapNode[size];
    }

    public boolean isEmpty(){
        return lastIndex == 0;
    }

    public HeapNode peek(){
        if(isEmpty()){
            return null;
        }
        return dataArray[0];
    }

    public HeapNode poll(){
        if(isEmpty()){
            return null;
        }
        HeapNode res = peek();
        delete(res);
        return res;
    }

    public void insert(HeapNode node){
        dataArray[lastIndex] = node;
        posArray[node.id] = lastIndex;
        swimUp(lastIndex);
        lastIndex ++;
    }

    public boolean delete(HeapNode node){
        /*
        When delete a node, swap it with last node.
        Then delete last node.
        Then heapify
        * */
        int pos = posArray[node.id];
        if(pos < 0) return false;
        dataArray[pos] = dataArray[lastIndex-1];
        posArray[dataArray[pos].id] = pos;
        dataArray[lastIndex-1] = null;
        posArray[node.id] = -1;
        lastIndex--;
        if(pos != 0 && comparator.compare(dataArray[pos].data, dataArray[(pos-1)/2].data) < 0){
            swimUp(pos);
        }else {
            sinkDown(pos);
        }
        return true;
    }

    public void swimUp(int pos){
        /*
        Swim child node up until heapy.
        child node position: pos
        parent node position: (pos-1)/2
        * */
        if(pos == 0 || (comparator.compare(dataArray[pos].data, dataArray[(pos-1)/2].data) >=0)){
            return;
        }
        swap(pos, (pos-1)/2);
        swimUp((pos-1)/2);
    }

    public void sinkDown(int pos){
        /*
        Sink parent node down when parent node's data change.
        Swap parent node and its max value child node until heapy.
        parent node position: pos
        left child node position: 2*pos+1
        right child node position: 2*pos+2
        * */
        // first find max value child
        int maxChildPos;
        if(2*pos+1 >= lastIndex){
            return;
        }
        if(2*pos+2 >= lastIndex){
            maxChildPos = 2*pos+1;
        }else{
            maxChildPos = (comparator.compare(dataArray[2*pos+1], dataArray[2*pos+2]) < 0)
                           ? 2*pos+1 : 2*pos+2;
        }
        if(comparator.compare(dataArray[maxChildPos].data, dataArray[pos])>=0){
            return;
        }
        swap(pos,maxChildPos);
        sinkDown(maxChildPos);
    }

    public void swap(int pos1, int pos2){
        HeapNode tmp = dataArray[pos1];
        dataArray[pos1] = dataArray[pos2];
        dataArray[pos2] = tmp;
        posArray[dataArray[pos1].id] = pos1;
        posArray[dataArray[pos2].id] = pos2;
    }

    public static void main(String[] args){
        Comparator comp = new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                return (Integer)o1 - (Integer)o2;
            }
        };
        Heap test = new Heap(100, comp);
        test.insert(new HeapNode(0,1));
        test.insert(new HeapNode(1,-1));
        System.out.println(test.peek().data);
        // System.out.println(Arrays.toString(test.posArray));
    }
}