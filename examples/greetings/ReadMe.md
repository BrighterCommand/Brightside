# Setup
We recommend you use a virtual environment to to run the examples
You will need to have RabbitMQ installed to use this sample

pip install -r requirements.txt

# Run
We need to run the receiver first, to create the queue; you can run in any order, once you have created that queue
The receiver will run until you terminate with CRTL+Z, so you will need two terminal windows for this to work.

python receiver.py
python sender.py YOUR_NAME

# ToDo
Switch this to use a Docker file





