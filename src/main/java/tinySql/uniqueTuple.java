package main.java.tinySql;

import main.java.storageManager.Field;
import main.java.storageManager.FieldType;
import main.java.storageManager.Tuple;

import java.util.ArrayList;
import java.util.List;

public class uniqueTuple implements Comparable<uniqueTuple>{
    private List<Field> fields;
    private List<String> selectedFieldNames;
    private Field key;

    public uniqueTuple(){}

    public uniqueTuple(Tuple tuple){
        // distinct comparision with all field
        fields = new ArrayList<>();
        for(int i = 0; i < tuple.getNumOfFields(); i++){
            fields.add(tuple.getField(i));
        }
    }

    public uniqueTuple(Field key){
        // distinct comparision with specified field
        this.key = key;
        fields = new ArrayList<>();
        fields.add(key);
    }

    public uniqueTuple(Tuple tuple, List<String> selectedFieldNames){
        fields = new ArrayList<>();
        this.selectedFieldNames = selectedFieldNames;
        for(String fieldName : selectedFieldNames){
            Field field = tuple.getField(fieldName);
            fields.add(field);
        }
    }

    public List<String> getSelectedFieldNames(){
        return selectedFieldNames;
    }

    public void setSelectedFieldNames(List<String> selectedFieldNames){
        this.selectedFieldNames = selectedFieldNames;
    }
    public List<Field> getFields(){
        return fields;
    }

    public void setFields(List<Field> fields){
        this.fields = fields;
    }

    public int compareTo(uniqueTuple tuple2){
        if(key.type == FieldType.STR20){
            return key.str.compareTo(tuple2.key.str);
        }else{
            return ((Integer)key.integer).compareTo(tuple2.key.integer);
        }
    }

    // override hashcode
    @Override
    public int hashCode(){
        String str = "";
        for(Field f:fields){
            str += f;
        }
        return str.hashCode();
    }

    @Override
    public boolean equals(Object obj){
        return obj != null && (obj instanceof uniqueTuple) && this.hashCode() == obj.hashCode();
    }
}
