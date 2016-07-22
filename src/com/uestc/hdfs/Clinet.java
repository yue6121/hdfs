package com.uestc.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


public class Clinet {
	NameNode nameNode;
	public Clinet(NameNode nn) {
		nameNode = nn;
	}
	
	public void write(String filename,String data)throws IOException{
		List<String> chunks = new ArrayList<>();
		int chunkloc = 1;
		int num_chunks = get_num_chunks(data);
		for(int i=0;i<num_chunks;i++){
			int low = i*nameNode.chunksize;
			int high = (i+1)*nameNode.chunksize>data.length()?data.length():(i+1)*nameNode.chunksize;
			chunks.add((String) data.subSequence(low, high));
		}
		/* 分配空间，更新元数据*/
		List<String> chunk_ids = nameNode.connect(filename, num_chunks);
		for(int i=0;i<chunk_ids.size();i++){
			//一个chunk备份两份，总共三份
			chunkloc = i%nameNode.num_datanodes;
			nameNode.datanodes.get(chunkloc).write(chunk_ids.get(i).toString(), chunks.get(i));
			chunkloc = (i+1)%nameNode.num_datanodes;
			nameNode.datanodes.get(chunkloc).write(chunk_ids.get(i).toString(), chunks.get(i));
			chunkloc = (i+2)%nameNode.num_datanodes;
			nameNode.datanodes.get(chunkloc).write(chunk_ids.get(i).toString(), chunks.get(i));
		}
	}

	private int get_num_chunks(String data) {
		return (int) Math.ceil(data.length()*1.0/nameNode.chunksize);
	}
	
	public String read(String filename)throws IOException {
		if(nameNode.exits(filename)){
			String data = new String();
			List<String> chunk_ids = (List<String>)nameNode.filemap.get(filename);
			for(String id:chunk_ids){
				int chunloc = nameNode.chunkmap.get(id);
				String data_temp = nameNode.datanodes.get(chunloc).read(id);
				if(data_temp.equals("-1")){
					data_temp = nameNode.datanodes.get((chunloc+1)%nameNode.num_datanodes).read(id);
					System.out.println("current chunk"+id+" in "+chunloc+" is broken");
					if(data_temp.equals("-1")){
						data_temp = nameNode.datanodes.get((chunloc+2)%nameNode.num_datanodes).read(id);
						System.out.println("current chunk"+id+" in "+(chunloc+1)+" is broken");
					}
				}
				data += data_temp;
			}
			return data;
		}else{
			System.out.println("File "+filename+" is not exist!");
			return "-1";
		}
	}
	
	public boolean delete(String filename){
		if(nameNode.exits(filename)){
			List<String> chunk_ids = (List<String>)nameNode.filemap.get(filename);
			for(String id:chunk_ids){
				int chunkloc = nameNode.chunkmap.get(id);
				nameNode.datanodes.get(chunkloc).delete(id);
				nameNode.datanodes.get((chunkloc+1)%nameNode.num_datanodes).delete(id);
			}
			nameNode.delete(filename);
			return true;
		}else{
			return false;
		}
	}
	
	public void list_files(){
		System.out.println("Files:");
		Iterator it = (Iterator)nameNode.filemap.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry me = (Entry) it.next();
			System.out.println(me.getKey());
		}
	}

}
