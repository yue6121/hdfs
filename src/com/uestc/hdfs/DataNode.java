package com.uestc.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;

public class DataNode implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 2L;
	String dir_name = null;
	public DataNode(int chunkloc) {
		super();
		dir_name = "."+File.separator+"DataNode"+File.separator+Integer.toString(chunkloc);
		File dir = new File(dir_name);
		//if (dir.exists()) {
		//	System.out.println("dir "+dir_name+" is exist!");
		//}
		dir.mkdirs();
	}
	
	public void write(String id,String data) throws IOException{
		FileOutputStream fos = new FileOutputStream(dir_name+File.separator+id);
		fos.write(data.getBytes());
		fos.close();
	}
	
	public String read(String id)throws IOException{
		FileInputStream fis;
		try {
			fis = new FileInputStream(dir_name+File.separator+id);
			byte[] buf = new byte[fis.available()];
			fis.read(buf);
			fis.close();
			return new String(buf);
		} catch (FileNotFoundException e) {
			return "-1";
		}
	}
	
	public boolean delete(String id){
		File file = new File(dir_name+File.separator+id);
		if(file.exists()){
			return file.delete();
		}else{
			return false;
		}
	}
}
