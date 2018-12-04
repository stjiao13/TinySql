package main.java.tinySql;

import java.io.*;
import java.io.InputStreamReader;

public class Interface {
    public void run(){
        try{
            System.out.println("Hi, this is our TinySQL project!");
            System.out.println("=================================");
            Main main = new Main();
            System.out.println();
            System.out.println("TinySQL:");
            System.out.println("To execute file, please enter: source absolute_path_to_file");
            System.out.println();
            System.out.println("Please enter your TinySQL command:");
            InputStreamReader inputStreamReader = new InputStreamReader(System.in);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String readString = bufferedReader.readLine();
            main.exec(readString);
            do {
                readString = bufferedReader.readLine();
                if (readString != null) {
                    main.exec(readString);
                }
            }
            while (readString != null);
        } catch (Exception e){
            System.out.println("IO error");
        }


    }

    public static void main(String[] args){
        Interface curinterface = new Interface();
        curinterface.run();
    }
}
