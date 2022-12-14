import random
import time
from datetime import datetime
import sys
from kafka import KafkaProducer

BROKER_LIST='localhost:9092'


# Topics to generate messages on
# 	
TOPICS = {
    'cust': {
        'topicName': 'customers',
        'partitions': [
            {
                'time': range(30,50),
                'type': 'randCustomer',
                'message': { 'customer': range(10,20), 'account': range(400,410) }
            },
            {
                'time': range(30,50),
                'type': 'randCustomer',
                'message': { 'customer': range(10,20), 'account': range(400,410) }
            }
        ]
    },
    'acct': {
        'topicName': 'accounts',
        'partitions': [
            {
                'time': range(30,50),
                'type': 'randAccount',
                'message': { 'customer': range(10,20), 'account': range(400,410) }
            },
            {
                'time': range(30,50),
                'type': 'randAccount',
                'message': { 'customer': range(10,20), 'account': range(400,410) }
            },
            {
                'time': range(30,50),
                'type': 'randAccount',
                'message': { 'customer': range(10,20), 'account': range(400,410) }
            }
        ]
    },
    'tran': {
        'topicName': 'transactions',
        'partitions': [
            {
                'time': range(1,40),
                'type': 'randNumberPair',
                'message': { 'first': range(400,412), 'second': range(1,125) }
            },
            {
                'time': range(1,40),
                'type': 'randNumberPair',
                'message': { 'first': range(400,412), 'second': range(1,125) }
            },
            {
                'time': range(1,40),
                'type': 'randNumberPair',
                'message': { 'first': range(400,412), 'second': range(1,125) }
            },
            {
                'time': range(1,40),
                'type': 'randNumberPair',
                'message': { 'first': range(400,412), 'second': range(1,125) }
            }
        ]
    },
    'visit': {
        'topicName': 'visits',
        'partitions': [
            # {
            #     'time': range(1,10),
            #     'type': 'randNumber',
            #     'message': range(1,1000)
            # },
            # {
            #     'time': range(1,10),
            #     'type': 'randNumber',
            #     'message': range(1,1000)
            # },
            # {
            #     'time': range(1,10),
            #     'type': 'randNumber',
            #     'message': range(1,1000)
            # },
            # {
            #     'time': range(1,10),
            #     'type': 'randNumber',
            #     'message': range(1,1000)
            # },
            # {
            #     'time': range(1,10),
            #     'type': 'randNumber',
            #     'message': range(1,1000)
            # }
        ]
    }
}

def runGenerator(log, topicName, partitionIdx):

    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=BROKER_LIST, value_serializer=str.encode)
    
    def msgRandNumber(range):
        def g():
            return random.randrange(range.start, range.stop+1)
        return g
    def msgRandNumberPair(config):
        firstG = msgRandNumber(config['first'])
        secondG = msgRandNumber(config['second'])
        def g():
            return f"{str(firstG())},{str(secondG())}"
        return g
    def msgRandMessage():
        names=["We","I","They","He","She","They"]
        verbs=["was", "is", "are", "were"]
        nouns=["playing a game", "watching television", "talking", "dancing", "speaking", "showering"]
        def g():
            return f"{names[random.randint(0,len(names)-1)]} {verbs[random.randint(0,len(verbs)-1)]} {nouns[random.randint(0,len(nouns)-1)]}"
        return g
    def msgRandEmail():
        names=["joe", "linda", "mary", "michael", "dennis", "steve", "alex", "pat"]
        domains=["gmail.com", "outlook.com", "icloud.com", "yahoo.com", "aol.com"]
        def g():
            return f"{names[random.randint(0,len(names)-1)]}@{domains[random.randint(0,len(domains)-1)]}"
        return g
    def msgRandBoolean():
        def g():
            return random.getrandbits(1) == 0
        return g
    def msgRandState():
        states=["AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
                "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
                "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"]
        def g():
            return states[random.randint(0,len(states)-1)]
        return g
    def msgRandAccount(config):
        accountG = msgRandNumber(config['account'])
        customerG = msgRandNumber(config['customer'])
        messageG = msgRandMessage()
        def g():
            return f"{accountG()},{customerG()},{messageG()}"
        return g
    def msgRandCustomer(config):
        customerG = msgRandNumber(config['customer'])
        emailG = msgRandEmail()
        prefG = msgRandBoolean()
        stateG = msgRandState()
        def g():
            return f"{customerG()},{emailG()},{prefG()},{stateG()}"
        return g

    topic = TOPICS[topicName]
    partition = topic['partitions'][partitionIdx]
    generator = None
    if partition['type'] == 'randNumber':
        generator = msgRandNumber(partition['message'])
    elif partition['type'] == 'randNumberPair':
        generator = msgRandNumberPair(partition['message'])
    elif partition['type'] == 'randAccount':
        generator = msgRandAccount(partition['message'])
    elif partition['type'] == 'randCustomer':
        generator = msgRandCustomer(partition['message'])
    else:
        raise "unknown generator type: " + partition['type']
    waitTimeG = msgRandNumber(partition['time'])
    while True:
        waitTime = waitTimeG()
        print(f"\t{topicName}({partitionIdx}): Waiting {waitTime} secs")
        time.sleep(waitTime)
        value = f"{str(generator())},{datetime.utcnow().isoformat()[:-3]}Z"
        print(f"{topicName}({partitionIdx}): Generated {value}")
        producer.send(topic['topicName'], value=value, partition=partitionIdx)
        
        
if __name__ == '__main__':

    from multiprocessing import Process, Queue

    total_threads = sum([len(TOPICS[k]['partitions']) for k,v in TOPICS.items()])
    print(f"Starting {total_threads} threads. (Enter x to exit)")

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
    

