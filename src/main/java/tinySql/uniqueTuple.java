package main.java.tinySql;

import main.java.storageManager.Field;
import main.java.storageManager.Tuple;

import java.util.ArrayList;
import java.util.List;

public class uniqueTuple implements Comparable<uniqueTuple>{
    private List<Field> fields;
    private List<String> selectedFieldNames;

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

    public int compareTo(uniqueTuple key){
        return 0;
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
