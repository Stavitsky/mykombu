from kombu import Exchange, Queue


task_exchange = Exchange("msgs", type="direct")
queues_dict = {}
queues_dict['first_VM'] = Queue("first_VM", task_exchange,
                                routing_key='TO_SERV_1')
queues_dict['second_VM'] = Queue("sevond_VM", task_exchange,
                                 routing_key='TO_SERV_2')

queue_msg_1 = Queue("first_VM", task_exchange, routing_key='TO_SERV_1')
queue_msg_2 = Queue("sevond_VM", task_exchange, routing_key='TO_SERV_2')
queue_msg_back = Queue("back", task_exchange, routing_key='TO_CLIENT')

queues_mass = [queue_msg_1, queue_msg_2]


def add_queue(uuid):

    queues_dict[str(uuid)] = Queue(str(uuid), task_exchange,
                                   routing_key=str(uuid))
    print ("[!!!] in queues.add_queue()_CREATED QUENQUE %s" % (uuid))
    print ("[!!!] queues_dict now: ")
    for (name, queue) in queues_dict.items():
        print name, queue

#TODO: add delete