from confluent_kafka import Producer, Consumer, KafkaError
import threading
import sys
import time
import os.path
from os import path

# initial topic to which the server will connect
topic= 'server'

# a set of active topics
active_topics= set()

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
    while my_input != 'exit':
        global topic
        print()
        my_input= input()

        if my_input == 'exit':
            p.poll(0)

        # logging out
        elif my_input == "logout":
            print ("Attempting to Logout..")
            my_input =my_input+ "-" + sys.argv[2]
            p.produce(topic, my_input.encode('utf-8'))
            time.sleep(5)
            print("Logged Out!")

        # else send a message to the client you have to reply to
        else:
            my_input =my_input+ "-" + sys.argv[2]
            p.produce(topic, my_input.encode('utf-8'), callback = delivery_report)

# the consumer thread
def consumer_function():
    c = Consumer({'bootstrap.servers': sys.argv[1], 'group.id': '1', 'auto.offset.reset': 'earliest'})
    c.subscribe([sys.argv[2]])

    # read the messages indefinetly
    while True:
        global topic
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        
        # decode the message consumed
        notification= msg.value().decode('utf-8')

        # check- if the client demands the list of active topics
        if notification.find('list existing topics') != -1:
            p= Producer({'bootstrap.servers': sys.argv[1]})
            params= notification.split(' ')

            # add the new client id(topic) to the set active topics
            topic= params[3]
            active_topics.add(topic)
            
            # prepare the list and publish it to the client
            list= " : ".join(active_topics)
            if list.find(": ") == -1:
                list += ": "
            p.produce(topic, (list.encode('utf-8')), callback= delivery_report)
            
            # prepare the message (remove the client id from the recieved message) to write to the log file
            notification= ""
            for i in range (0, len(params)-1):
                notification += params[i] +" "

            address= params[3]+ ".txt"

            # create and write to a file the message recieved to maintain log
            if path.exists(address) == True:
                file= open(address, "a")
                file.write(notification)
                file.close()
            # append the list if the file already exists
            else:
                file= open(address, "w")
                file.write(notification)
                file.close()

        # check- if the client requested to log out: remove it from the list of active topics
        elif notification.find('logout') != -1:
            params= notification.split("-")
            address= params[1]
            active_topics.discard(address)

        # append the message consumed normally to the chat history log
        else:
            params= notification.split("-")
            address= params[1]
            topic= address
            active_topics.add(topic)
            address += ".txt"

            notification= ""
            for i in range (0, len(params)-1):
                notification += params[i] +" "

            print('Received message from {} : {}'.format(params[1] ,notification))
            print()

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



