package com.uestc.hdfs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Scanner;

public class Command {
	Clinet client = null;
	
	public Command(Clinet c) {
		client = c;
	}
	
	public void command_line()throws IOException{
		while(true){
			System.out.println("Input the Command like 'upload' 'download' 'delete' 'ls' 'exit'");
			Scanner scanner = new Scanner(System.in);
			String cmd = scanner.nextLine();
			switch (cmd) {
			case "upload":
				upload_cmd();
				continue;
			case "download":
				download_cmd();
				continue;
			case "delete":
				delete_cmd();
				continue;
			case "ls":
				client.list_files();
				System.out.println();
				continue;
			case "exit":
				System.exit(0);

			default:
				System.out.println("Wrong command!");
				continue;
			}
		}
	}

	private void delete_cmd() {
		System.out.println("Input the filename which you want to delete in HDFS:");
		Scanner scanner =  new Scanner(System.in);
		String filename = scanner.nextLine();
		client.delete(filename);
		System.out.println("delete sucess!");
	}

	private void download_cmd() throws IOException {
		System.out.println("Input the filename which you want to download in HDFS:");
		Scanner scanner =  new Scanner(System.in);
		String filename = scanner.nextLine();
		String data = client.read(filename);
		System.out.println(data);
		//write local
		FileOutputStream fos = new FileOutputStream(filename);
		fos.write(data.getBytes());
		fos.close();
		System.out.println("download success!");
	}

	private void upload_cmd() throws IOException {
		System.out.println("Input the filename which you want to upload in local:");
		Scanner scanner =  new Scanner(System.in);
		String filename = scanner.nextLine();
		FileInputStream fis;
		try {
			fis = new FileInputStream(filename);
			byte[] data = new byte[fis.available()];
			fis.read(data);
			fis.close();
			client.write(filename,new String(data));
			System.out.println("upload success!");
		} catch (FileNotFoundException e) {
			System.out.println("No such file in local!");
		}
	}
}
