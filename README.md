# PortScan-Detection
This is a port-scan detection model using CICIDS-2017-Friday NetFlow data for training and running on the HDFS

## Getting Started
* Follow [my start-up process](https://coding-ray.notion.site/Startup-Project-Word-Count-87159b0e950d4a50a266c239c172459c) to set up the environment and hadoop

## Usful Command Sets
* Reset the file system HDFS if nessesary:  
    * Format the HDFS. It is useful when there is an DataStreamer exception
    (which tells the user that data is written to none of the datanodes.)  
    `make reset_hdfs`.
    * Erase everything, including the formatted HDFS.
    It is useful when there is an exception of connection refused
    (maybe because that the IP of the localhost is changed in the router)
    and cannot be solved by simply re-format the HDFS.  
    `make deep_reset_hdfs`

* Start everything of the HDFS:  
  `sudo start-all.sh`

* Put the input data to the HDFS:  
  `make put_data`

* Compile the program:  
  `make`

* Run the program:  
  `make run_psd`

* Clear the console, compile and run the program:  
  `make all`

* Run the test program locally:  
  `make testlocal`