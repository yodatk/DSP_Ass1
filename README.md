Tomer Gonen - 311200331
Elimor Cohen - 301727657

# Intro:

In this assignment we coded a real-world application to distributively apply OCR algorithms on images.
The application is composed of a local application and instances running on the Amazon cloud.
The application will get as an input a text file containing a list of URLs of images. those images will be sent ot an Manager Instance in AWS , which will assign workers for that assignment.
Then, instances will be launched in AWS (workers). Each worker will download image files, use some OCR library to identify text in those images (if any) and display the image with the text in a webpage.

We will now explain how each of the moudules is working: LocalApplication, Mananger and Worker.

- There are also JavaDoc files in the 'JavaDocForAssignment1' folder. Open the index.html file to view all the information on the different modules and classes, with descreption on each field and function

# Local Application:

The local Application is the client of the program. it's sends an input of images urls to be parsed online, and recived the Images and their parsed text from the images.
first thing the local application does is checking if there is a manager instance that is already running in the cloud. if so, it will send it the file with all the urls to parse.
Otherwise - it will create an instance of a manger and Queue to register to manager services. for the LocalApplication to register to manager services, it needs to send the Message in the registration queue with the following details:

1. localId for the current application.
2. LocalApplication queue name to send the finished task notification
3. s3 bucket name to send the images with their parsed text
4. s3 object key to the inut file with all the urls to parse
5. number of urls to parse
6. n - number of images per worker this assignment requires.

After sending all these details, the local application waits for an message for the manager that the job is done.
When it's done, it will recieve a message from the manager with the number of temp files it should download from s3(will be explained shortly)
The LocalApplication downloads all the data and uses the HTMLParser - Class that we wrote in the LocalApplication module that incharge of writting the parsed results into the html file.
after reading all the lines that were recivend by the manager, the html output file is done. if the the Local application was initialize with the Terminate flag, it will send the Terminate message to the manager, and than stop working

# Manager

manager is created by the first LocalApplication that is in need of it's services. The LocalApplication laready created the registration queue and send a registration message there for the manager to handle.
in out project the Manager application is consists from 3 threads. each thread is in charge of a different task:

## 1st task: Handle Registration Messages:

This thread is in charge of getting registration messages from local applications , to check all the given paramteres by them and procces the relavant data.
it's running in the same loop :
first, this thread checks if there are any registration messages. if so, it downloads the input data from the s3 , and puts all the given url in the input file in the TO DO task of the workers.
each url will also be assign with the local app id to match the url to it's local application after the parse is done.
Afterwards, it will check the 'n' given by the LocalApplication, and calculates if there enough workers that are running for the current task.
if there are workers missing, it will creates more workers until it will satisfy the given LocalApplication 'n'
when the manager recived Terminate message fomr one of the local application, it will stop the 1st thread.

## 2nd task: Handle Parsed Images from workers:

2nd thread is listening to a different queue in SQS from AWS, that is where all workers are puttin their finished work in.
For each parsed image that come through, the worker also will sent the local app that requested it, so the manager could count another task that is done for that LocalApplication.
if the parsed data is more that the manager can handle in it's memory for the current local application,
it will send all the given data as a Long String Object in s3 and marked it as a temp file for the local application to read later.
this thread is also in charge of updating the number of tasks left for each local application when a message arrive from one of the workers,
so the manager will know when a task is finished(3rd task)
as long as the manager is noit terminated by one of the local app , or as long there are tasks to be done, this thread will keep running

## 3rd task: Handle Finished Task For LocalApplications:

3rd thread is checking contantly if there are tasks from locals that are finished.
If so, it will check that all the temp files for the local application are saved properly, and sent the local application an sqs message to notify the local application that the job is done.
afterwards it will delete all the meta data that was saved for that local application, and contrinue listening for messages from k
as long as the manager is noit terminated by one of the local app , or as long there are tasks to be done, this thread will keep running

after all tasks are done and terminated message arrive, the main thread will terminate all the workers, and deletes all remaining queues from SQS service, and after that the manager will stop it's running.

# Worker

Worker is created by the manager Instance. each time the manager calculates there are not enough workers for the given images, it will create more instances of Worker.
worker is listening for the TO DO queue and when a messaege arrives from there, it will Parse it with the OCRParser - another Class we wrote for parsing text from images using the tesseract library.
for each parsed text from url, the worker send the images to the queue that is where all workers are puttin their finished work in, and then the worker continue to listen to the queue of the TO DO list by the worker.
the Worker loop will continue to run until the manager terminates all the workers instances.

# Configuration data

## Images:

image of the manager: includes aws-linux, aws-cli tools, and java 1.8
image of the worker : includes ubuntu, aws-cli tools, java 1.8, and the tassaract data.

## Instances types:

all AWS instances in the program are of type T-Micro. the reasons are:
Manager: The manager is the main "bottle neck" of the program. it is important that it will run fast, so T.NANO is not enough.
Worker : Although it's task seems not very heavy, the libery we used (tassaract) is pretty heavy, and also required from us to install Ubuntu linux, which is heavier from aws-linux.
from experiments we've learned that the T.NANO is much slower than the micro , so we switch the worker to be a T.Micro as well.

## RunTime:

We Ran several expirements with the data, here are the results:

### Test 1

Local application #1: n=10, size of input file: 57 urls
Local application #2: n=10, size of input file: 57 urls
Number of workers created: 6
Total run time (from starting local application until receving both files): ~ 3 minutes

### Test 2

Local application #1: n=20, size of input file: 114 urls
Local application #2: n=20, size of input file: 114 urls
Number of workers created: 6
Total run time (from starting local application until receving both files): ~ 3 minutes

### Test 3

Local application #1: n=10, size of input file: 57 urls
Local application #2: n=9, size of input file: 57 urls
Local application #3: n=8, size of input file: 57 urls
Local application #4: n=7, size of input file: 57 urls
Local application #5: n=7, size of input file: 57 urls
Local application #6: n=7, size of input file: 57 urls
Number of workers created: 9
Total run time (from starting local application until receving both files): ~ 4 minutes

### test 4

Local application #1: n=180, size of input file: 1000 urls
Number of workers created: 6
Total run time (from starting local application until receving both files): ~ 5 minutes

For all tests, output file were correct and valid.

# How to run the program :

Each part of the program has it's own maven project. for each Sub-Project to create jar executables,
write the command `mvn clean package` in each of the sub directory and you'll have the executable
jar of that module. to run the program on the AWS services, you'll need to have credentials for AWS,
and upload the images of the manager and the worker to s3 ( same images that were described before).
enter the directory of \dsp211Ass1LocalApplication\target,
and than run the local app argument in the same way that was decribed in the assignment:
java -jar dsp211Ass1LocalApplication-1.0-SNAPSHOT-shaded.jar <inputFileName> <outputFileName> <n> <terminate>
when:
inputfileName - name of the file where are all the urls that needs to be parse
outputfileName - name of the html file of the images with their parsed text
n - number of images to parse per worker
terminate- optional parameter to stop the manager from getting more tasks from local applications
(or run the provided jar we supplied for the local application)

## If you want to create other jars from the program :

### Manager:

From the root directory of the project, write
`cd dsp211Ass1Manager`
Afterwards, write the command
`mvn clean package`
The jar of the manager will be in the target folder.

### Worker:

From the root directory of the project, write
`cd dsp211Ass1Worker`
Afterwards, write the command
`mvn clean package`
The jar of the manager will be in the target folder.
