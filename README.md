# LT5G_DB_support_tool
This tool generates configuration and label data file for LT5G complying with their format. 

LabelData.json is a sample labeled data JSON in the current directory of this repository  

# Execute the file push_data_T_tables.py to launch the UI.  

# Sync the labeled data to oracle,stay on 'JSON to Oracle' tab.
Browse for the file in your system, the file is internally validated.    
  - The FolderName should be the ticket ID of the recording (2018080708035211893) 
  - It will return the project, function, TAN and recording name of the ticket ID.  
  - Hit the 'Sync' button to sync the data to their respective tables.  

# To generate Label schema and Labeled data file  
  - Browse to the 'Oracle to JSON' tab. 
  - Enter the config id by selecting the radio button, it will only generate the label schema file. 
  - Enter the ticket id for the recording, it will generate both the label schema and labeled data. 
  - One can select a location to save the files in their local system.
  - Hit the 'Retrieve' button and wait till a pop-up indicates that the schema/labeled files have been generated.

## APIs
  - The APIs are defined in the file lt5_server/lt5_support_server.py
  - Flask, a micro web framework is used to develop the REST APIs to request data from oracle.
  - The folder 'lt5_server' can be hosted on to a system using apache web server or Nginx.
  - It can be independently run as a microservice.
  - Each API is defined in a separate class and a sample URL is given in the doc string of every 'get' function of the class.
  
Note: Prior to this, run the lt5_server/lt5_support_server.py    
A stand-alone server comes in action requesting data to oracle by the APIs that are defined.
