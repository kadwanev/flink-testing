import random
import time
import sys
from kafka import KafkaProducer

BROKER_LIST='localhost:9092'


# Topics to generate messages on
# 	
TOPICS = {
    'ACCT': {
        'name': 'accounts',
        'partitions': [
            {
                'time': range(30,50), 
                'type': 'randNumber',
                'message': range(5,20)
            },
            {
                'time': range(30,50),
                'type': 'randNumber',
                'message': range(0,5)
            }
        ]
    },
    'TRAN': {
        'name': 'transactions',
        'partitions': [
            {
                'time': range(1,40),
                'type': 'randNumber',
                'message': range(1,500)
            },
            {
                'time': range(1,40),
                'type': 'randNumber',
                'message': range(1,300)
            },
            {
                'time': range(1,40),
                'type': 'randNumber',
                'message': range(1,100)
            },
            {
                'time': range(1,40),
                'type': 'randNumber',
                'message': range(1,500)
            },
            {
                'time': range(1,40),
                'type': 'randNumber',
                'message': range(30,300)
            }
        ]
    }
}

def runGenerator(log, topicName, partitionIdx):

    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=BROKER_LIST, value_serializer=str.encode)
    
    def msgRandNumber(range):
        def g():
            msg = random.randrange(range.start, range.stop)
            return msg
        return g
    
    header = str.format("%s(%d)" % (topicName, partitionIdx))
    topic = TOPICS[topicName]
    partition = topic['partitions'][partitionIdx]
    generator = msgRandNumber(partition['message'])
    while True:
        waitTime = random.randint(partition['time'].start, partition['time'].stop)
        print(str.format("%s: Waiting %d secs" % (header, waitTime)))
        time.sleep(waitTime)
        value = str(generator())
        print(str.format("%s: Generated %s" % (header, value)))
        producer.send(topic['name'], value=value, partition=partitionIdx)
        
        

if __name__ == '__main__':

    from multiprocessing import Process, Queue

    total_threads = sum([len(TOPICS[k]['partitions']) for k,v in TOPICS.items()])
    print(str.format("Starting %d threads. (Enter x to exit)" % total_threads))

    log = Queue()
    processes = []
    for topic in TOPICS:
        for partition, _ in enumerate(TOPICS[topic]['partitions']):
            p = Process(target=runGenerator, args=(log, topic,partition,))
            p.start()
            processes.append(p)
#    processes[0].join()
    for input in sys.stdin:
        if ('x' == input.rstrip()):
            for p in processes:
                p.terminate()
            sys.exit()
    

