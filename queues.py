from kombu import Exchange, Queue
task_exchange = Exchange("msgs", type="direct")
queues_mass = []
queue_msg_1 = Queue("first_VM", task_exchange, routing_key='TO_SERV_1')
queue_msg_2 = Queue("sevond_VM", task_exchange, routing_key='TO_SERV_2')
queue_msg_back = Queue("back", task_exchange, routing_key='TO_CLIENT')

queues_mass.append(queue_msg_1)
queues_mass.append(queue_msg_2)


def add_queue(routing_key):
    n = len(queues_mass)
    tmp_queue = Queue("tmp_queue"+str(n),
                      task_exchange, routing_key=routing_key)
    queues_mass.append(tmp_queue)
    print ("!!!_in queues.add_queue()_CREATED QUENQUE N"+str(queues_mass.index(tmp_queue)))
    return queues_mass.index(tmp_queue)
