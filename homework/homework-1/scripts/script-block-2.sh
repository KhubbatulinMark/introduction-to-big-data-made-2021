hdfs dfs -mkdir /newfolder
hdfs dfs -mkdir /newfolder/subfolder
# Trash - корзина удаленых файлов. Нужна чтобы восстановить случайно удаленные файлы
# Чтобы миновать это нужно использовать параметр -skipTrash
hdfs dfs -touchz /newfolder/subfolder/newfile.txt
hdfs dfs -rm -skipTrash /newfolder/subfolder/newfile.txt
hdfs dfs -rm -skipTrash -r /newfolder
hdfs dfs -put deutschland.txt /
hdfs dfs -cat /deutschland.txt
hdfs dfs -cat /deutschland.txt | tail
hdfs dfs -cat /deutschland.txt | head -n 2
hdfs dfs -cp /deutschland.txt /newfolder/deutchland.txt
hdfs dfs -setrep -w 1 /deutschland.txt # Заняло секунд 14
hdfs dfs -setrep -w 3 /deutschland.txt # Заняло секунд 8
hdfs fsck /deutschland.txt -files -blocks -locations
hdfs fsck -blockId blk_1073741830

