package com.uestc.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class HdfsTest {
	public static void main(String[] args) throws IOException {
		NameNode nn = nameNodeDeserial();
		Clinet c = new Clinet(nn);
		Command command = new Command(c);
		command.command_line();
	}
	
	public static NameNode nameNodeDeserial(){
		File file = new File("namenode.serial");
		ObjectInputStream ois = null;
		NameNode namenode = null;
		if (file.exists()) {
			try {
				ois = new ObjectInputStream(new FileInputStream("namenode.serial"));
				namenode = (NameNode) ois.readObject();
				ois.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		} else {
			namenode = new NameNode();
		}

		return namenode;
	}
}
