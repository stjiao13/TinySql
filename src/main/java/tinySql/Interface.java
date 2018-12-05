package main.java.tinySql;

import java.io.*;
import java.io.InputStreamReader;

public class Interface {
    public void run(){
        try{
            System.out.println("Hi, this is our TinySQL project!");
            System.out.println("=================================");
            Main main;
            System.out.println();
            System.out.println("TinySQL:");
            System.out.println("To execute file, please enter: source absolute_path_to_file");
            System.out.println();
			System.out.println("To execute file, please enter: source absolute_path_to_file > output_flie_name");
			System.out.println();
			System.out.println("To execute single sql statement, just enter the statement");
			System.out.println();
			System.out.println("Please enter your TinySQL command: ");
            InputStreamReader inputStreamReader = new InputStreamReader(System.in);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String readString = bufferedReader.readLine();
			// judge the type of command - read a file or single sql statement
            String[] splits = readString.split(" ");
            int index = splits[0].indexOf("source");
            if (index >= 0) {
				// read and output
				if (splits.length > 2) {
					String filepath = splits[1];
					String outputFilename = splits[3];
					System.out.println("filepath = " + filepath);
					System.out.println("outputFilename = " + outputFilename);
					main = new Main(outputFilename);
					main.parseFile(filepath);
				}
				// just read
				else {
					main = new Main();
					String filepath = splits[1];
					main.parseFile(filepath);
				}
				main.pw.close();
				System.out.println("Query Success! Run interface again if you need another query.");
				System.out.println();
				bufferedReader.close();
				return;
			}

			main = new Main();
            main.exec(readString);
            do {
                readString = bufferedReader.readLine();
                if (readString != null) {
                    main.exec(readString);
                }
            }
            while (readString != null);
			System.out.println();
			System.out.println("Query Success! Run interface again if you need another query.");
			return;
		} catch (Exception e){
            System.out.println("IO error");
        }


    }

    public static void main(String[] args){
        Interface curinterface = new Interface();
        curinterface.run();
    }
}
