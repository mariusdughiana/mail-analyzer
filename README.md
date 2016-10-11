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

<img width="1920" alt="screen shot 2016-10-10 at 23 54 52" src="https://cloud.githubusercontent.com/assets/4759627/19265131/c6a81aea-8f9b-11e6-979e-f1807e2cc2be.png">


<img width="1920" alt="screen shot 2016-10-10 at 23 55 10" src="https://cloud.githubusercontent.com/assets/4759627/19265187/fc69c354-8f9b-11e6-85c7-29ca0e00b843.png">
