package com.uestc.hdfs;

import java.net.*;
import java.io.*;

/**
 * 使用socket Server实现DataNode。一直处于监听状态，对传输的数据进行处理。
 * 对收到的Object(String [])数据进行判断，数据中arr[0]为传输的命令；arr[1]为文件名；arr[2]为chunk文件内容。
 * 其中，arr[1]和arr[2]为可选项。
 * 根据arr[0]的 不同，作出不同反应：
 * 1.write:此时，arr[1]为文件名(chunk_uuid)，arr[2]为chunk文件内容。将内容写入该文件中。
 * 2.read：读取文件名(chunk_uuid)的内容并返回。
 * 3.delete：删除文件名(chunk_uuid)的内容并返回是否成功。
 * 注：输入其他命令时，会提示“Wrong command”
 * @author DanielJyc
 *
 */
public class DataNodeServer extends Thread {
	private Socket server;
	private String path ="."+ File.separator + "DataNode1" + File.separator;

	/**
	 * 构造函数，初始化：为每一个DataNodeServer创建不同的目录，存放chunk数据。
	 * @param ser
	 */
	public DataNodeServer(Socket ser) {
		this.server = ser;
		File dir = new File(path);
		if(dir.exists()){
			System.out.println("目录" + path + "存在");
		}
		dir.mkdirs();
	}

	/**
	 * 对收到的Object(String [])数据进行判断，数据中arr[0]为传输的命令；arr[1]为文件名；arr[2]为chunk文件内容。
	 * 其中，arr[1]和arr[2]为可选项。
	 * 根据arr[0]的 不同，作出不同反应：
	 * 1.write:此时，arr[1]为文件名(chunk_uuid)，arr[2]为chunk文件内容。将内容写入该文件中。
	 * 2.read：读取文件名(chunk_uuid)的内容并返回。
	 * 3.delete：删除文件名(chunk_uuid)的内容并返回是否成功。
	 * 注：输入其他命令时，会提示“Wrong command”
	 */
	public void run() {
		try {
			ObjectOutputStream out = new ObjectOutputStream(server.getOutputStream());
			ObjectInputStream in = new ObjectInputStream(server.getInputStream());
			// 接收--Mutil User but can't parallel
			String [] arr= (String[]) in.readObject();
			System.out.println(arr[0]+arr[1]);
			switch (arr[0]) {
			case "write":
				System.out.println("Write command.");
				write(arr[1], arr[2]);
				break;
			case "read":				
				System.out.println("Read command.");
				String data = read(arr[1]); //从本地读取文件内容
				String[] s =new String[]{data};
				out.writeObject(s);  //发送
				out.flush();			
				break;
			case "delete":
				System.out.println("Read command.");
				String[] s1 = null;
				if(true == delete(arr[1])){		//从本地读取文件内容
					s1 =new String[]{"Delete done."};
				}
				else {
					s1 =new String[]{"Filename dose not exits."};
				}
				out.writeObject(s1);  //发送
				out.flush();
				break;
			default:
				System.out.println("Wrong command.");
				break;
			}
			out.close();
			in.close();
			server.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	/**
	 * delete：删除文件名(chunk_uuid)的内容并返回是否成功。
	 * @param chunk_uuid
	 * @return 删除成功返回true，否则返回false
	 */
	private boolean delete(String chunk_uuid) {
		// TODO Auto-generated method stub
		File file = new File( path+chunk_uuid);
		if (!file.exists()) { // 不存在，返回false
			return false;
		} else {
			return file.delete();
		}
	}

	/**
	 * 2.read：读取文件名(chunk_uuid)的内容并返回。
	 * @param chunk_uuid
	 * @return 返回文件内容。 读取失败，返回标识-1，告诉Client从下一个DataNode读取。
	 */
	private String read(String chunk_uuid)  {
		// TODO Auto-generated method stub
		FileInputStream fis;
		try {
			fis = new FileInputStream(path+chunk_uuid);
			byte[] buf = new byte[fis.available()];// 定义一个刚刚好的缓冲区。
			fis.read(buf);
			fis.close();
			return new String(buf);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			return "-1"; // 文件不存在范围-1，从而方便从下一个读取
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "-1"; 
		}
	}

	/**
	 * 1.write:此时，arr[1]为文件名(chunk_uuid)，arr[2]为chunk文件内容。将内容写入该文件中。
	 * @param chunk_uuid  arr[1]为文件名(chunk_uuid)
	 * @param chunk  arr[2]为chunk文件内容
	 * IOException ：文件不存在的时候，输出："The file in HDFS is broken."
	 */
	private void write(String chunk_uuid, String chunk)  {
		// TODO Auto-generated method stub
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(path+chunk_uuid);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("The file in HDFS is broken.");
		}
		try {
			fos.write(chunk.getBytes());
			fos.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 主程序入口：监听socket，接收数据；启动线程。
	 * @param args
	 */
	@SuppressWarnings("resource")
	public static void main(String[] args)  {
		ServerSocket server = null;
		try {
			server = new ServerSocket(12346);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		while (true) {
			// transfer location change Single User or Multi User
			DataNodeServer ser = null;
			try {
				ser = new DataNodeServer(server.accept());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			ser.start();
		}
	}
}
