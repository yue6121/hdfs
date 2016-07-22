package com.uestc.hdfs;

import java.io.IOException;

public class HdfsTest {
	public static void main(String[] args) throws IOException {
		NameNode nn = new NameNode();
		Clinet c = new Clinet(nn);
		Command command = new Command(c);
		command.command_line();
	}
}
