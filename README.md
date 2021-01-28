# OCR & AWS Distributed Systems Programing
This program gets an input a .txt file with links to pictures of text and outputs an HTML file with the pictures followed by the text written in them using Tesseract OCR and AWS.

# How the program works:
Local App first checks the correctness of the arguments.
Then it creates an SQS queue for itself with a unique name (formatted
with the current date).
After, it checks if there is an active manager. If it does not find
one, it creates one and puts it in the first S3 bucket, also it creates
a queue for the manager. If it finds some manager, it takes its URL
queue and saves it.
Then it creates a unique S3 bucket for itself and uploads there its
URL queue, n value, and the input file.
Then it sends a new task message to the manager queue with its bucket
name (the manager will use the bucket name to distinguish between the
different locals).
Now, the local waits for an answer from the manager. When it gets the
"done task" message it takes the summary file from its own bucket (the
manager put it there as a string) and creates the HTML output file.
After it created the output file, it looks for a 'terminate' argument.
If it exists, it sends a terminate message to the manager queue,
otherwise do nothing, delete its own bucket, and queue and finish the
running.

The Manager instance starts to run after some local created it. First,
it takes the URL of its own queue from the first bucket (for listening
to it later). After, it creates another queue for workers. Then it
starts listening to its queue.
When it gets the 'new task' message, it downloads the URLs file and
n value from the local bucket. Now, for each line, it sends a new image
task to the worker's queue with the name of the local bucket. Also,
it creates the required number of workers according to the n value.
Then, it saves in the local bucket the amount of the URLs, the number
of workers it created for, and a blank summary file as a string.
When it gets the 'done OCR task' message it downloads the summary file
from the local bucket (worker sends the OCR output with the original
URL and the name of the local). Then it adds the OCR output to the 
summary file. Now it updates the number of URLs in the local bucket
and checks if there are other URLs to wait for. If yes, it uploads
the updated summary file to the bucket. If no, it sends terminate
messages to the worker queues (according to the number of workers it
created for this local). Then it closes the summary file and sends
a message to the local queue that it finished.
When it gets a 'terminate' message, it first checks if there are some
other locals that waiting for an answer. If yes, it continues in a
loop until it finishes to go over all URLs of all locals. If not, it
first closes all SQS queues, then deletes the remained buckets, and
in the end, terminates itself.

The Worker instance starts after the manager created it. First, it
gets the URLs of the manager and worker queue from the first bucket.
Then it starts listening to its own queue.
When it gets the 'new image task' message it takes the URL and
executes the 'Tesseract' algorithm on it. If the algorithm returns
a valid answer, it sends a 'done OCR image' message to the manager
queue with the URL, the OCR output, and the local. If the algorithm
returns an exception it catches it and sends a 'done OCR image' message
with the error as the OCR output.
When it gets the 'terminate' message, it terminates itself.

# EC2 Instances and image used for this program :
In this program I used t2 micro instance type, plus an AMI that includes the
tesseract trained data, java 11, javac 11, amazon CLI, and tesseract.
