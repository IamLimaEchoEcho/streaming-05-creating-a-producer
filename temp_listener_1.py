"""
#
Lee Jones 
Module 05 - A5  

    This program listens for time/temps from three smoker/food queues.  
    It will generate alert messages if any specific events occur
    1.) The smoker temperature decreases by more than 15 degrees F in 2.5 minutes (smoker alert!)
    2.) Any food temperature changes less than 1 degree F in 10 minutes (food stall!)
    

"""

import pika
import sys
import time
import csv
from datetime import datetime

#set queue names
queue1 = "01-smoker"
queue2 = "02-food-A"
queue3 = "03-food-B"

#create lists
lst_smoker_time_temp = []
lst_fooda_time_temp = []
lst_foodb_time_temp = []


#3 callbacks - one for each queue we're monitoring 

# define a callback function to be called when a smoker message is received
def smoker_callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    # decode the binary message body to a string
    print(f" [x] Smoker Received: {body.decode()}")
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    #ch.basic_ack(delivery_tag=method.delivery_tag)

    #save smoker temps over time
    smoker_current_str = body.decode()
    smoker_current_list = smoker_current_str.split(", ")
    if smoker_current_list[0] != '' and smoker_current_list[1] != '':
        lst_smoker_time_temp.append(smoker_current_list)

        #date/time format is mm/dd/yy hh:mm:ss
        smoker_current_time = datetime.strptime(smoker_current_list[0], '%m/%d/%y %H:%M:%S')
        smoker_current_temp = float(smoker_current_list[1])

        #read backwards through list and test each time/temp
        i = len(lst_smoker_time_temp) - 1
        while i >= 0: 
            #for j in range(len(lst_smoker_time_temp[i])):
            smoker_test_time = datetime.strptime(lst_smoker_time_temp[i][0], '%m/%d/%y %H:%M:%S')
            smoker_test_temp = float(lst_smoker_time_temp[i][1])

            if smoker_test_temp - smoker_current_temp >= 15:
                    date_time_diff = smoker_current_time - smoker_test_time 
                    date_time_diff_m = date_time_diff.total_seconds() / 60
                    if date_time_diff_m <= 2.5:
                        print(f" !!! Smoker alert! Temp decrease by 15+ dgrees in 2.5 minutes. {smoker_test_temp} to {smoker_current_temp}")

            i-= 1

def fooda_callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    # decode the binary message body to a string
    print(f" [x] FoodA Received: {body.decode()}")
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    #ch.basic_ack(delivery_tag=method.delivery_tag)
    fooda_current_str = body.decode()
    fooda_current_list = fooda_current_str.split(", ")
    if fooda_current_list[0] != '' and fooda_current_list[1] != '':
        lst_fooda_time_temp.append(fooda_current_list)

        #date/time format is mm/dd/yy hh:mm:ss
        fooda_current_time = datetime.strptime(fooda_current_list[0], '%m/%d/%y %H:%M:%S')
        fooda_current_temp = float(fooda_current_list[1])

        #read backwards through list and test each time/temp
        ii = len(lst_fooda_time_temp) - 1
        while ii >= 0: 

            fooda_test_time = datetime.strptime(lst_fooda_time_temp[ii][0], '%m/%d/%y %H:%M:%S')
            fooda_test_temp = float(fooda_current_list[ii][1])
            if abs(fooda_current_temp) - abs(fooda_test_temp) <= 1:
                date_time_diff = fooda_current_time - fooda_test_time 
                date_time_diff_m = date_time_diff.total_seconds() / 60
                if date_time_diff_m <= 10:
                    print(f" !!! FoodA stall alert! FoodA temp has not increased by 1+ degree in in 10 minutes. {fooda_test_temp} to {fooda_current_temp}")



def foodb_callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    # decode the binary message body to a string
    print(f" [x] FoodB Received: {body.decode()}")
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    #ch.basic_ack(delivery_tag=method.delivery_tag)
    foodb_current_str = body.decode()
    foodb_current_list = foodb_current_str.split(", ")
    if foodb_current_list[0] != '' and foodb_current_list[1] != '':
        lst_foodb_time_temp.append(foodb_current_list)

        #date/time format is mm/dd/yy hh:mm:ss
        foodb_current_time = datetime.strptime(foodb_current_list[0], '%m/%d/%y %H:%M:%S')
        foodb_current_temp = foodb_current_list[1]

        #read backwards through list and test each time/temp
        iii = len(lst_fooda_time_temp) - 1
        while iii >= 0:  
            foodb_test_time = datetime.strptime(lst_foodb_time_temp[iii][0], '%m/%d/%y %H:%M:%S')
            foodb_test_temp = float(foodb_current_list[iii][1])
            if abs(foodb_current_temp) - abs(foodb_test_temp) <= 1:
                date_time_diff = foodb_current_time - foodb_test_time 
                date_time_diff_m = date_time_diff.total_seconds() / 60
                if date_time_diff_m <= 10:
                    print(f" !!! FoodA stall alert! FoodA temp has not increased by 1+ degree in in 10 minutes. {foodb_test_temp} to {foodb_current_temp}")


# define a main function to run the program
def main(hn: str = "localhost", qn1: str = queue1, qn2: str = queue2, qn3: str = queue3):
    """ Continuously listen for task messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        channel = connection.channel()

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        channel.queue_declare(queue=qn1, durable=True)
        channel.queue_declare(queue=qn2, durable=True)
        channel.queue_declare(queue=qn3, durable=True)

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        channel.basic_qos(prefetch_count=1) 

        # configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        channel.basic_consume( queue=qn1, on_message_callback=smoker_callback, auto_ack=True)
        channel.basic_consume( queue=qn2, on_message_callback=fooda_callback, auto_ack=True)
        channel.basic_consume( queue=qn3, on_message_callback=foodb_callback, auto_ack=True)

        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    #main("localhost", "task_queue2")
    main()
