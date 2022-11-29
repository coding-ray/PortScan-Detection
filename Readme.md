# Port-Scan Detection

## Getting Started
* Follow [my start-up process](https://coding-ray.notion.site/Startup-Project-Word-Count-87159b0e950d4a50a266c239c172459c) to set up the environment and hadoop

## Usful Command Sets
* To start the everything of the HDFS:  
  `sudo start-all.sh`
* To reset the file system HDFS:  
    * Format the HDFS. It is useful when there is an DataStreamer exception
    (which tells the user that data is written to none of the datanodes.)  
    `make reset_hdfs`.
    * Erase everything, including the formatted HDFS.
    It is useful when there is an exception of connection refused
    (maybe because that the IP of the localhost is changed in the router)
    and cannot be solved by simply re-format the HDFS.  
    `make deep_reset_hdfs`
* To compile the program:  
  `make`
* To put the input data:  
  `make put_data`
* To run the program:  
  `make run_psd`
* To run the test program:  
  `make testlocal`