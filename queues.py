from kombu import Exchange, Queue
task_exchange = Exchange("msgs", type="direct")
queue_msg_1 = Queue("first_VM", task_exchange, routing_key='TO_SERV_1')
queue_msg_2 = Queue("sevond_VM", task_exchange, routing_key='TO_SERV_2')
queue_msg_back = Queue("back", task_exchange, routing_key='TO_CLIENT')
