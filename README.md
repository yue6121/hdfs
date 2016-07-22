# hdfs

# 实现一个简单的hdfs，包括文件的上传，下载，删除，查看文件目录等功能。
# CommandLine负责将命令传给客户端Client，客户端调用NameNode和DataNode实现上述功能。
# 一个NameNode作为Mater，管理文件目录以及元数据信息，四个DataNode负责保存数据。
# 数据上传到hfs，会按照chunk_size进行分块，每个数据备份三个存在不同的DataNode中。
