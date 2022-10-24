import socket
import threading
from random import randint, sample
from numpy import sort


class individual:

    def __init__(self, idx, is_byz, init_op, ex_thr, ip, port):
        ##### The individual's index
        self.idx = idx
        ##### The individual's opinion. It can be either inside {0,1}, or -1, which means that it
        ##### has no opinion.
        self.opinion = init_op
        ##### The value that the individual broadcasts in phase two
        self.phase_two_value = []
        ##### The opinion of the individual at the end of each round
        self.rnd_opinions = [init_op]
        ##### The counter for the first phase of each round
        self.first_phase_cntr = []
        ##### The counter for the second phase of each round
        self.second_phase_cntr = []
        ##### A flag indicating whether the individual has decided or not
        self.decided = False
        ##### A flag indicating that the first phase has finished
        self.phase_one_is_finished = [False]
        ##### A flag indicating that the handler should ignore phase one messages
        self.ignore_phase_one_messages = []
        ##### A flag indicating that the handler should ignore phase two messages
        self.ignore_phase_two_messages = []
        ##### A flag indicating if the individual is Byzantine
        self.is_byz = is_byz
        ##### A flag indicating whether the node is listening for requests or not
        self.is_listening = True
        ##### The active queries in each query round
        self.active_queries = []
        ##### The responses received in phase one
        self.phase_one_messages = []
        ##### The responses received in phase two
        self.phase_two_messages = []
        ##### The query round
        self.query_rnd = 0
        ##### The phase in each round
        self.phase = 1
        ##### The execution threshold: the individual should not run indefinitely, and should
        ##### decide after enough time has passed.
        self.ex_thr = ex_thr
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
        self.phase_one_messages_lock = threading.Lock()
        self.phase_one_is_finished_lock = threading.Lock()
        self.phase_two_messages_lock = threading.Lock()
    
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
            if num_decided == num_individuals:
                break
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
        #while not self.decided:
        while True:
            if num_decided == num_individuals:
                break
            if not self.phase_one_is_finished[self.query_rnd]:
                self.query_rnd = self.query_rnd + 1
                indiv_sample = sample_individuals(self.idx)
                while not len(indiv_sample) == 0:
                    if num_decided == num_individuals:
                        break
                    successful_queries = []
                    for indiv in indiv_sample:
                        with self.opinion_lock:
                            #print(f"{self.idx} - rnd_opinions length is {len(self.rnd_opinions)}")
                            #print(f"self.query_rnd - 1 is {self.query_rnd - 1}")
                            if self.is_byz:
                                success = send_msg(indiv, "1:" + str(self.query_rnd) + ":" + str(randint(0, 1)))
                            else:
                                if self.decided:
                                    success = send_msg(indiv, "1:" + str(self.query_rnd) + ":" + str(self.opinion))
                                else:
                                    success = send_msg(indiv, "1:" + str(self.query_rnd) + ":" + str(self.rnd_opinions[self.query_rnd - 1]))
                        if success:
                            successful_queries.append(indiv)
                    for indiv in successful_queries:
                        indiv_sample.remove(indiv)
                with self.phase_one_is_finished_lock:
                    ############### Dangerous!!!! TODO:
                    self.phase_one_is_finished.append(True)
                    #self.phase_one_is_finished[self.query_rnd - 1] = True
                if self.decided:
                    self.phase = 2
                    self.phase_two_value.append(-1)
            if self.phase == 2:
                indiv_sample = sample_individuals(self.idx)
                while not len(indiv_sample) == 0:
                    if num_decided == num_individuals:
                        break
                    successful_queries = []
                    for indiv in indiv_sample:
                        with self.opinion_lock:
                            if self.is_byz:
                                success = send_msg(indiv, "2:" + str(self.query_rnd) + ":" + str(randint(0, 1)))
                            else:
                                if self.decided:
                                    success = send_msg(indiv, "2:" + str(self.query_rnd) + ":" + str(self.opinion))
                                else:
                                    success = send_msg(indiv, "2:" + str(self.query_rnd) + ":" + str(self.phase_two_value[self.query_rnd - 1]))
                        if success:
                            successful_queries.append(indiv)
                    for indiv in successful_queries:
                        indiv_sample.remove(indiv)
                self.phase = 1
                if self.decided:
                    self.phase_one_is_finished[self.query_rnd] = False
    
    def handler(self, msg:str, addr):
        splitted_msg = msg.split(":")
        msg_rnd = int(splitted_msg[1])
        if splitted_msg[0] == "1":
            with self.phase_one_messages_lock:
                while len(self.ignore_phase_one_messages) < msg_rnd:
                    self.ignore_phase_one_messages.append(False)
                
                while len(self.first_phase_cntr) < msg_rnd:
                    self.first_phase_cntr.append(0)
                
                while len(self.phase_one_messages) < msg_rnd:
                    self.phase_one_messages.append([])
                
                while len(self.phase_two_value) < msg_rnd:
                    self.phase_two_value.append(-1)
                
                if not self.ignore_phase_one_messages[msg_rnd - 1] and not self.decided:
                    self.first_phase_cntr[msg_rnd - 1] = self.first_phase_cntr[msg_rnd - 1] + 1
                    self.phase_one_messages[msg_rnd - 1].append(int(splitted_msg[2]))
                    if self.first_phase_cntr[msg_rnd - 1] >= num_individuals - t:
                        self.ignore_phase_one_messages[msg_rnd - 1] = True
                        one_cntr = 0
                        zero_cntr = 0
                        for val in self.phase_one_messages[msg_rnd - 1]:
                            if val == 0:
                                zero_cntr += 1
                            elif val == 1:
                                one_cntr += 1
                        #print(f"{self.idx} --- phase one - phase one messages are {self.phase_one_messages[msg_rnd - 1]}")
                        #print(f"{self.idx} --- phase one - zero_cntr is {zero_cntr}")
                        #print(f"{self.idx} --- phase one - one_cntr is {one_cntr}")
                        
                        if zero_cntr >= (num_individuals + t) / 2:
                            self.phase_two_value[msg_rnd - 1] = 0
                        elif one_cntr >= (num_individuals + t) / 2:
                            self.phase_two_value[msg_rnd - 1] = 1
                        else:
                            self.phase_two_value[msg_rnd - 1] = -1
                        #print(f"{self.idx} --- phase one - phase two value is {self.phase_two_value[msg_rnd - 1]}")
                        
                        if self.query_rnd == msg_rnd:
                            self.phase = 2
        
        elif splitted_msg[0] == "2":
            with self.phase_two_messages_lock:

                while len(self.ignore_phase_two_messages) < msg_rnd:
                    self.ignore_phase_two_messages.append(False)
                
                while len(self.second_phase_cntr) < msg_rnd:
                    self.second_phase_cntr.append(0)
                
                while len(self.phase_two_messages) < msg_rnd:
                    self.phase_two_messages.append([])
                ####### The <= is very important! self.rnd_opinions starts with a value in
                ####### the first element, and is not initially empty.
                while len(self.rnd_opinions) <= msg_rnd:
                    self.rnd_opinions.append(-1)
                
                with self.phase_one_is_finished_lock:
                    while len(self.phase_one_is_finished) <= msg_rnd:
                        self.phase_one_is_finished.append(True)
                
                if not self.ignore_phase_two_messages[msg_rnd - 1] and not self.decided:
                    self.second_phase_cntr[msg_rnd - 1] += 1
                    self.phase_two_messages[msg_rnd - 1].append(int(splitted_msg[2]))
                    if self.second_phase_cntr[msg_rnd - 1] >= num_individuals - t:
                        #print(f"{self.idx} entered phase 2.")
                        self.ignore_phase_two_messages[msg_rnd - 1] = True
                        minus_one_cntr = 0
                        zero_cntr = 0
                        one_cntr = 0
                        for val in self.phase_two_messages[msg_rnd - 1]:
                            if val == 0:
                                zero_cntr += 1
                            elif val == 1:
                                one_cntr += 1
                            else:
                                minus_one_cntr += 1
                        #print(f"{self.idx} --- phase two - zero_cntr is {zero_cntr}")
                        #print(f"{self.idx} --- phase two - one_cntr is {one_cntr}")
                        #print(f"{self.idx} --- phase two - phase two messages are {self.phase_two_messages[msg_rnd-1]}")
                        global num_decided    
                        if self.phase_two_value[msg_rnd - 1] == 0 and zero_cntr >= (num_individuals + t) / 2:
                            self.rnd_opinions[msg_rnd] = 0
                                
                            if self.query_rnd == msg_rnd and not self.is_byz:
                                self.opinion = 0
                                self.decided = True
                                with num_decided_lock:
                                    num_decided += 1
                                print(f"Individual {self.idx} decided 0 - Round opinions are {self.rnd_opinions}")
                            with self.phase_one_is_finished_lock:
                                self.phase_one_is_finished[msg_rnd] = False

                        elif self.phase_two_value[msg_rnd - 1] == 0 and zero_cntr >= t + 1:
                            self.rnd_opinions[msg_rnd] = 0
                            with self.phase_one_is_finished_lock:
                                self.phase_one_is_finished[msg_rnd] = False
                            #if self.query_rnd == msg_rnd:
                            #    pass

                        elif self.phase_two_value[msg_rnd - 1] == 1 and one_cntr >= (num_individuals + t) / 2:
                            self.rnd_opinions[msg_rnd] = 1
                                
                            if self.query_rnd == msg_rnd and not self.is_byz:
                                self.opinion = 1
                                self.decided = True
                                with num_decided_lock:
                                    num_decided += 1
                                print(f"Individual {self.idx} decided 1 - Round opinions are {self.rnd_opinions}")
                            with self.phase_one_is_finished_lock:
                                self.phase_one_is_finished[msg_rnd] = False

                        elif self.phase_two_value[msg_rnd - 1] == 1 and one_cntr >= t + 1:
                            self.rnd_opinions[msg_rnd] = 0
                            with self.phase_one_is_finished_lock:
                                self.phase_one_is_finished[msg_rnd] = False
                            #if self.query_rnd == msg_rnd:
                            #    pass
                            
                        else:
                            self.rnd_opinions[msg_rnd] = randint(0, 1)
                            with self.phase_one_is_finished_lock:
                                self.phase_one_is_finished[msg_rnd] = False
                                
                            
                        #if self.query_rnd == msg_rnd:
                        #    self.opinion = self.rnd_opinions[msg_rnd - 1]
                        #    self.phase_one_is_finished = False




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
    addresses = []
    for idx in pruned_individuals:
        addresses.append(("127.0.0.1", individual_ports[idx]))
    return addresses



num_individuals = 6
t = 1
port_base = 5060
ex_thr = 5
alpha = 1
beta = 2
k = 3
individual_ports = []
individual_opinions = []
individuals = {}
threads = []
indivs = []

manual_initial_opinion = [1, 1, 0, 0, 1, 1, 0, 1, 0, 1, 0, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0]
open("round_opinions_Ben-Or.txt", "w").close()

for i in range(num_individuals):
    individual_ports.append(port_base + i)
    #individual_opinions.append(randint(-1, 1))
    
    individual_opinions.append(randint(0, 1))
    #individual_opinions.append(manual_initial_opinion[i])

byz_indices = sort(sample(list(range(num_individuals)), t))

print(f"Byzantine nodes are: {byz_indices}")

num_decided = len(byz_indices)
num_decided_lock = threading.Lock()

for i in range(num_individuals):
    if i in byz_indices:
        indivs.append(individual(i, 1, individual_opinions[i], ex_thr, "127.0.0.1", individual_ports[i]))
    else:
        indivs.append(individual(i, 0, individual_opinions[i], ex_thr, "127.0.0.1", individual_ports[i]))
    threads.append(threading.Thread(target=indivs[i].listener, args=()))
    threads[-1].start()
print(f"Initial opinions are {individual_opinions}")
while num_decided < num_individuals:
    continue
print("Everyone decided!")