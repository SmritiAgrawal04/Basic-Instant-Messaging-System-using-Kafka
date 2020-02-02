from confluent_kafka import Producer, Consumer, KafkaError
import threading
import sys
import time
import os.path
from os import path

# initial topic to which the client will connect
topic= 'server'

# a set of contacts made by the client
contact_list= set()

# a report to have acknowledge of the message delivery
def delivery_report(error, message):
    if error:
        print('Message delivery failed: {}'.format(error))
    else:
        print('Message delivered to {}'.format(message.topic()))

# the producer thread
def producer_function():
    p= Producer({'bootstrap.servers': sys.argv[1]})

    my_input= None

    # produce while client asks to exit
    while my_input != 'exit':
        global topic
        print()
        print ("**Enter your Message for {}**".format(topic))
        my_input= input()
        if my_input == 'exit':
            p.poll(0)
            exit

        # inform the server about the logging out activity
        elif my_input == "logout":
            print ("Attempting to Logout..")
            my_input =my_input+ "-" + sys.argv[2]
            p.produce(topic, my_input.encode('utf-8'))
            time.sleep(5)
            print("Logged Out!")

        # retrieve the chat history of the client with another client
        elif my_input.find('read') != -1:
            print ("Enter the client whose messages you want to read: ")
            client= input()
            print ("Retrieving Messages..")
            time.sleep(5)
            client += ".txt"
            try:
                file= open(client, "r")
                print (file.read())
            except FileNotFoundError:
                print ("OOPS! You haven't done any chat to the requested Client.")
            finally:
                    file.close()

        # ask the server for active topics and connect to either of them or server itself
        elif my_input.find('list existing topics') != -1:
            my_input =my_input+ " " + sys.argv[2]
            p.produce('server', my_input.encode('utf-8'))
            print ("Please Wait... Processing your Request!")
            print ("Existing Topics: ")
            time.sleep(5)
            my_input= input()
            topic= my_input
            print ("You chose to connect to: {}".format(topic))
            contact_list.add(my_input)

        # connect to the server from any stage
        elif my_input == 'connect to server':
            print("Attempting to Connect..")
            time.sleep(5)
            topic= 'server'
            print ("Connection Successful!")

        # retrieve the list of contacts
        elif my_input == 'contact list':
            temp_topic = topic
            topic= sys.argv[2]
            for contact in contact_list:
                print (contact)
            topic= temp_topic

        # else send a message to the client/server you want to chat to
        else:
            my_input =my_input+ "-" + sys.argv[2]
            p.produce(topic, my_input.encode('utf-8'), callback = delivery_report)

# the consumer thread
def consumer_function():
    c = Consumer({'bootstrap.servers': sys.argv[1], 'group.id': '1', 'auto.offset.reset': 'earliest'})
    c.subscribe([sys.argv[2]])

    # read the messages indefinetly
    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        
        # decode the message consumed
        notification= msg.value().decode('utf-8')

        # check- if client consumed the list of active topics
        if notification.find(":") != -1:

            # create and write to a file the list recieved to maintain log
            if path.exists("server.txt") == True:
                file= open("server.txt", "a")
                file.write(notification)
                file.close()
            # append the list if the file already exists
            else:
                file= open("server.txt", "w")
                file.write(notification)
                file.close()

            # print the active topics for the client
            current_topics = notification.split(": ")
            for user in current_topics:
                print (user)
            print ("Or choose 'server' to connect to Server")
            print ("Make your choice: ")

        # append the message consumed normally to the chat history log
        else:
            params= notification.split("-")
            address= params[1]
            address += ".txt"

            notification= params[0]

            print('Received message from {} : {}'.format(params[1] ,notification))

            # create and write to a file the message recieved to maintain log
            if path.exists(address) == True:
                file= open(address, "a")
                file.write(notification)
                file.write("\n")
                file.close()

            # append the message if the file already exists
            else:
                file= open(address, "w")
                file.write(notification)
                file.write("\n")
                file.close()


if __name__ == "__main__":

    produce= threading.Thread(target= producer_function)
    consume= threading.Thread(target= consumer_function)

    # start the producer and consumer threads
    produce.start()
    consume.start()

    produce.join()
    consume.join()



