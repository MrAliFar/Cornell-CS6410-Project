import socket
import threading
from random import randint, sample


class individual:

    def __init__(self, idx, init_op, ex_thr, alpha, beta, ip, port):
        ##### The individual's index
        self.idx = idx
        ##### The individual's opinion. It can be either inside {0,1}, or -1, which means that it
        ##### has no opinion.
        self.opinion = init_op
        ##### The opinion of the individual at the end of each round
        self.rnd_opinions = []
        ##### The counter moving towards alpha
        self.alpha_cntr = 0
        ##### The counter moving towards beta
        self.beta_cntr = 0
        ##### A flag indicating whether the individual has decided or not
        self.decided = False
        ##### A flag indicating if the individual is Byzantine
        self.is_byz = False
        ##### A flag indicating whether the node is listening for requests or not
        self.is_listening = True
        ##### The active queries in each query round
        self.active_queries = []
        ##### The queries that have received a response in a round
        self.responded_queries = []
        ##### The query round
        self.query_rnd = 0
        ##### The execution threshold: the individual should not run indefinitely, and should
        ##### decide after enough time has passed.
        self.ex_thr = ex_thr
        ##### Alpha and Beta are safety parameters, related to Snowflake's probabilistic safety
        ##### guarantees.
        self.alpha = alpha
        self.beta = beta
        ##### The IP address and the port of the individual
        self.ip = ip
        self.port = port
        ##### A flag indicating whether the individual should continue to be active or not
        if not self.opinion == -1:
            self.is_active = True
        else:
            self.is_active = False
        ##### The locks guarding the individual's internal state
        self.opinion_lock = threading.Lock()
        self.decided_lock = threading.Lock()
        self.is_active_lock = threading.Lock()
        self.responded_queries_lock = threading.Lock()
    
    def spawn_handler(self, msg, addr):
        new_handler = threading.Thread(target=self.handler, args=(msg, addr,))
        new_handler.start()
    
    def listener(self):
        my_poller = threading.Thread(target=self.poller, args=())
        my_poller.start()

        sck = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sck.bind((self.ip, self.port))
        while self.is_listening:
            #with self.decided_lock:
            #    if self.decided:
            #        break
            sck.listen()
            conn, addr = sck.accept()
            response_received = False
            response = ""
            while not response_received:
                data = conn.recv(1024)
                #print(data)
                if not data:
                    response_received = True
                    continue
                response = response + data.decode()
            conn.close()
            self.spawn_handler(response, addr)
    
    def poller(self):
        while not self.decided:
            if self.is_active:
                self.query_rnd = self.query_rnd + 1
                indiv_sample = sample_individuals(self.idx)
                while not len(indiv_sample) == 0:
                    successful_queries = []
                    for indiv in indiv_sample:
                        with self.opinion_lock:
                            success = send_msg(indiv, "query:" + str(self.opinion) + ":" + str(self.port))
                        if success:
                            #print(f"{self.idx} sent query to {indiv[1]}")
                            successful_queries.append(indiv)
                    for indiv in successful_queries:
                        indiv_sample.remove(indiv)
                ###### Dangerous!!!!!!!!!!!!!!!!!!!!!!! TODO
                with self.is_active_lock:
                    #print(f"{self.idx} finished query round {self.query_rnd} with opinion {self.opinion}")
                    self.is_active = False
        #exit(0)
    
    def handler(self, msg:str, addr):
        splitted_msg = msg.split(":")
        if splitted_msg[0] == "query":
            if self.opinion == -1:
                self.is_active = True
            with self.opinion_lock:
                if self.opinion == -1:
                    self.opinion = int(splitted_msg[1])
            success = False
            while not success:
                success = send_msg((addr[0], int(splitted_msg[2])), "response:" + str(self.opinion) + ":" + str(self.port))
            #print(f"{self.idx} sent response {str(self.opinion)} to {(addr[0], int(splitted_msg[2]))}")
        elif splitted_msg[0] == "response":
            #print(f"{self.idx} received response from {splitted_msg[2]}")
            with self.responded_queries_lock:
                self.responded_queries.append((addr, int(splitted_msg[1])))
                #print(f"{self.idx} ---- len is {len(self.responded_queries)}")
                if len(self.responded_queries) == k:
                    #print(f"{self.idx} received k responses for query round {self.query_rnd}")
                    for item in self.responded_queries:
                        if item[1] == self.opinion:
                            self.alpha_cntr = self.alpha_cntr + 1
                    self.responded_queries = []
                    if self.alpha_cntr >= self.alpha:
                        self.beta_cntr = self.beta_cntr + 1
                    else:
                        self.beta_cntr = 0
                        self.opinion = int(not self.opinion)
                    
                    self.rnd_opinions.append(self.opinion)
                    
                    self.alpha_cntr = 0
                    if self.beta_cntr >= self.beta:
                        #print(f"{self.idx} decided {self.opinion} at round {self.query_rnd}!")
                        print(f"{self.idx} round opinions are: {self.rnd_opinions}")
                        self.beta_cntr = 0
                        self.decided = True
                        str_to_write = ' '.join(str(item) for item in self.rnd_opinions)
                        f = open("round_queries.txt", "a")
                        f.write(str_to_write + "\n")
                        f.close()
                    with self.is_active_lock:
                        self.is_active = True
            
            #print(f"Individual {self.port} received response {msg} from {addr}")
                

        #exit(0)




def send_msg(addr, val:str):
    #print(f"Address is {addr}")
    sck = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sck.connect(addr)
        sck.send(val.encode())
        sck.close()
        #print("Done!")
        return True
    except:
        #print("None!")
        return False

def sample_individuals(indiv_idx):
    pruned_individuals = list(range(num_individuals))
    pruned_individuals.remove(indiv_idx)
    indiv_indices = sample(pruned_individuals, k)
    addresses = []
    for idx in indiv_indices:
        addresses.append(("127.0.0.1", individual_ports[idx]))
    return addresses



num_individuals = 30
port_base = 5010
ex_thr = 5
alpha = 2
beta = 2
k = 3
individual_ports = []
individual_opinions = []
individuals = {}
threads = []

open("round_queries.txt", "w").close()

for i in range(num_individuals):
    individual_ports.append(port_base + i)
    #individual_opinions.append(randint(-1, 1))
    individual_opinions.append(randint(0, 1))
for i in range(num_individuals):
    indiv = individual(i, individual_opinions[i], ex_thr, alpha, beta, "127.0.0.1", individual_ports[i])
    threads.append(threading.Thread(target=indiv.listener, args=()))
    threads[-1].start()
print(f"Initial opinions are {individual_opinions}")