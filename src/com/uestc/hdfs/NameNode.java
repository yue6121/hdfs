package com.uestc.hdfs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class NameNode {
	int num_datanodes = 4;//data数目
	int chunksize = 10;//块大小
	Map<String,List<String>> filemap = new HashMap<>();//filename->chunk_ids
	Map<String,Integer> chunkmap = new HashMap<>();//chunkid->chunkloc
	Map<Integer,DataNode> datanodes = new HashMap<>();//chunkloc->datanode
	
	public NameNode() {
		init_datanodes();
	}
	/*init datanodes*/
	public void init_datanodes() {
		for(int i=0;i<num_datanodes;i++){
			DataNode dataNode = new DataNode(i);
			datanodes.put(i, dataNode);
		}
	}
	/* filemap and chunkmap*/
	public List<String> connect(String filename,int num_chunks){
		int chunkloc =1;
		String chunk_id = null;
		List<String> chunk_ids = new ArrayList<String>();
		for(int i=0;i<num_chunks;i++){
			chunk_id = UUID.randomUUID().toString();
			chunk_ids.add(chunk_id);
			chunkmap.put(chunk_id, chunkloc);
			chunkloc = chunkloc % num_datanodes + 1;
		}
		filemap.put(filename, chunk_ids);
		return chunk_ids;
	}
	/*delete file*/
	public void delete(String filename){
		List<String> chunk_ids = (List<String>)filemap.get(filename);
		for(String id:chunk_ids){
			chunkmap.remove(id);
		}
		filemap.remove(filename);
	}
	
	public boolean exits(String filename){
		Iterator<String> iterator = filemap.keySet().iterator();
		while(iterator.hasNext()){
			String temp = (String)iterator.next();
			if(filename.equals(temp))
				return true;
		}
		return false;
	}
	
}
