#!/bin/bash

# Environment variables

## Local
TEST_DIR = test_local
#INPUT_DATA_PATH = data/cicids2017_friday.nf
INPUT_DATA_PATH = data/NetFlow.nf
WHITELIST_PATH = whitelist/*
SRC = src/*.java
OUT_DIR = out
CLASS_FILE_DIR = $(OUT_DIR)/psd

## Hadoop
APP_NAME = psd.jar # Port-Scan Detection
CLASS_CONTAINING_MAIN = Entry # Class (File) name that contains the main function
DFS_DATANODE_DATA_DIR = /home/hadoop # in /usr/etc/hadoop/hdfs-site.xml

## HDFS
HDFS_INPUT_PATH = psd/input/1
HDFS_OUTPUT_PATH = psd/output/1
HDFS_WHITELIST_PATH = psd/whitelist

#-------------------------------------------------------------------#

# Useful Command Sets

## Local: Compile the final program into a jar
$(APP_NAME): $(CLASS_FILE_DIR) $(SRC)
	@javac -classpath `hadoop classpath` -d $(CLASS_FILE_DIR) $(SRC)
  # equivalent to "hadoop com.sun.tools.javac.Main $(SRC)"
	@cd $(CLASS_FILE_DIR); \
	jar -cf ../$(APP_NAME) .


## Local: Clear all compiled files and the profram
cleanup:
	rm out/* -rf


## Local: Make the folder to contain the compiled files
$(CLASS_FILE_DIR):
	mkdir -p $(CLASS_FILE_DIR)


## Local: Test for local java program
test: $(TEST_DIR)/PlayGround.java
	@javac $(TEST_DIR)/*.java
	@java $(TEST_DIR)/PlayGround
	@rm $(TEST_DIR)/*.class


## Local: Check the disk usage of HDFS
check_du:
	@sudo du --human-readable --summarize $(DFS_DATANODE_DATA_DIR) /tmp/hadoop* |\
	sort --human-numeric-sort --reverse


## Both: Run all tasks that should be done to run the PSD \
         after "sudo start-all.sh"
all:
	clear
	make $(APP_NAME) --no-print-directory
	make run_psd --no-print-directory 2>$(OUT_DIR)/job-message.txt


## HDFS: Run the program
run_psd: $(OUT_DIR)/$(APP_NAME)
	@hdfs dfs -rm -r -f $(HDFS_OUTPUT_PATH)
	@cd $(OUT_DIR); \
	hadoop jar $(APP_NAME) $(CLASS_CONTAINING_MAIN) $(HDFS_INPUT_PATH) $(HDFS_OUTPUT_PATH)
	@make get_data --no-print-directory


## Both: Erase everythong of the HDFS even the formated file system \
        and initialize the HDFS
deep_reset_hdfs: stop_hdfs
	sudo rm -rf /home/hadoop
	make reset_hdfs --no-print-directory


## HDFS: Format the HDFS
reset_hdfs:
	sudo hdfs namenode -format
	sudo start-all.sh
	sudo hdfs dfs -mkdir /user /user/root /user/ray
	sudo hdfs dfs -chown ray /user/ray
	hdfs dfs -mkdir -p $(HDFS_INPUT_PATH) $(HDFS_WHITELIST_PATH)


## Both: Stop everything of the HDFS
stop_hdfs:
	sudo stop-all.sh


## Both: Copy data from local to the HDFS
put_data: data
	hdfs dfs -put -f $(INPUT_DATA_PATH) $(HDFS_INPUT_PATH)
	hdfs dfs -put -f $(WHITELIST_PATH) $(HDFS_WHITELIST_PATH)


## HDFS: Remove input data in the HDFS
remove_data:
	hdfs dfs -rm -r -f $(HDFS_INPUT_PATH)* $(HDFS_WHITELIST_PATH)*


## Both: Copy output from HDFS to local
get_data:
	hdfs dfs -copyToLocal -f $(HDFS_OUTPUT_PATH) $(OUT_DIR)


## Local: Print git log
log:
	@git log --oneline --graph --all