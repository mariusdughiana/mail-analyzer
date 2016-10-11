# mail-analyzer

## Prerequisites:
 - Java 8
 - Maven 3
 - Git
 
 ## Build:
 
 ```bash
      https://github.com/mariusdughiana/mail-analyzer.git
      mvn clean install
 ``` 
 
 ### Run
 
 ` java -jar mail-analyzer-1.0-SNAPSHOT-jar-with-dependencies.jar path_to_data_as_xml`
 
 ### Caveats
- works with xml data format
- all the data has to be unzipped in the same folder and the path to that folder will be used as a parameter when starting the app
- there is room for improvment on how the average words per mail is being computed as there are files which contain chain of emails, hence some email messages are duplicated and affects the overall average. Also to extract only the message from those text files is also a challenge, in terms of identifying all the possible patterns. 

### Running on EC2
