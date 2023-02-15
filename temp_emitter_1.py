"""
#
Lee Jones 
Module 05 - A5  

    This program uses an input file to send messages to a queue on the RabbitMQ server.
    Make tasks harder/longer-running by adding dots at the end of the message.

    Author: Denise Case
    Date: January 15, 2023

"""

import pika
import sys
import webbrowser
import csv
import time

#Delay in receiving each temp reading
sleep_secs = 1
#show the RabbitMQ admin website
show_offer = False
#input file
myfile = 'smoker-temps.csv'


#set queue names
queue1 = "01-smoker"
queue2 = "02-food-A"
queue3 = "03-food-B"


def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    if show_offer: 
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        print(f" [x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site
    if show_offer: 
        offer_rabbitmq_admin_site()
    # get the message from the command line
    # if no arguments are provided, use the default message
    # use the join method to convert the list of arguments into a string
    # join by the space character inside the quotes
    #message = " ".join(sys.argv[1:]) or "Second task....."
    

    #declare tuples
    smoker_time_temp = ()
    foodA_time_temp = ()
    foodB_time_temp = ()
    
    with open(myfile, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            #hold the date/time
            smoker_time = row['Time (UTC)']
            #load tuples for smoker and food temps with timestamp
            smoker_time_temp = row['Time (UTC)'], row['Channel1']
            foodA_time_temp = row['Time (UTC)'], row['Channel2']
            foodB_time_temp = row['Time (UTC)'], row['Channel3']
            
            ##I'm not sure how to send a tuple, so creating strings to send
            #prepare strings
            smoker_temp = row['Channel1']
            foodA_temp = row['Channel2']
            foodB_temp = row['Channel3']
            #prepare binary message to stream
            smoker_message = f"{smoker_time}, {smoker_temp}"
            smoker_message_bin = smoker_message.encode()
            foodA_message = f"{smoker_time}, {foodA_temp}"
            foodA_message_bin = foodA_message.encode()
            foodB_message = f"{smoker_time}, {foodB_temp}"
            foodB_message_bin = foodB_message.encode()

            #send message 1 (smoker time/temp)
            if smoker_temp != '':
                send_message("localhost",queue1,smoker_message_bin)
                message = queue1

            #send message 2 (foodA time/temp)
            if foodA_temp != '':
                send_message("localhost",queue2,foodA_message_bin)
                message = queue2

            #send message 3 (foodB time/temp)
            if foodB_temp != '':
                send_message("localhost",queue3,foodB_message_bin)
                message = queue3

            #wait 30 seconds to read next line
            time.sleep(sleep_secs)