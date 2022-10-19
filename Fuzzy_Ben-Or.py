from pprint import pprint
import numpy as np
import socket
import threading
from random import randint, uniform


class individual:

    def __init__(self, idx, init_op, ex_thr, membership_param_zero, membership_param_half, membership_param_one, threshold_zero, threshold_one, ip, port):
        ##### The individual's index
        self.idx = idx
        ##### The individual's opinion. It can be either inside {0,1}, or -1, which means that it
        ##### has no opinion.
        self.opinion = init_op
        ##### The value that the individual broadcasts in phase two
        self.phase_two_value = []
        ##### The membership value of the average obtained in phase one for the "zero"
        self.phase_two_membership_zero = []
        ##### The membership value of the average obtained in phase one for the "half"
        self.phase_two_membership_half = []
        ##### The membership value of the average obtained in phase one for the "one"
        self.phase_two_membership_one = []
        ##### The opinion of the individual at the end of each round
        self.rnd_opinions = [init_op]
        ##### The final aggregated opinion after receiving enough phase 2 messages
        self.agg_opinions = []
        ##### The "zero" membership function parameter
        self.membership_param_zero = membership_param_zero
        ##### The "half" membership function parameter
        self.membership_param_half = membership_param_half
        ##### The "one" membership function parameter
        self.membership_param_one = membership_param_one
        ##### The "zero" defuzzification threshold
        self.threshold_zero = threshold_zero
        ##### The "one" defuzzification threshold
        self.threshold_one = threshold_one
        ##### The slope for the "zero" membership line y = ax + b
        self.zero_a = 1 / (1 - self.membership_param_zero)
        ##### The b for the "zero" membership line y = ax + b
        self.zero_b = -1 * self.membership_param_zero / (1 - self.membership_param_zero)
        ##### The slope for the "one" membership line y = ax + b
        self.one_a = 1 / (2 - self.membership_param_one)
        ##### The b for the "one" membership line y = ax + b
        self.one_b = self.membership_param_one / (self.membership_param_one - 2)
        ##### The slope for the left "half" membership line y = ax + b
        self.left_half_a = 1 / (1.5 - self.membership_param_half[0])
        ##### The b for the left "half" membership line y = ax + b
        self.left_half_b = 2 * self.membership_param_half[0] / (2 * self.membership_param_half[0] - 3)
        ##### The slope for the right "half" membership line y = ax + b
        self.right_half_a = 1 / (1.5 - self.membership_param_half[1])
        ##### The b for the right "half" membership line y = ax + b
        self.right_half_b = 2 * self.membership_param_half[1] / (2 * self.membership_param_half[1] - 3)
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
        self.is_byz = False
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
    
    def get_membership_zero(self, val):
        if val < 1:
            return 1
        elif val >= self.membership_param_zero:
            return 0
        else:
            return self.zero_a * val + self.zero_b
    
    def get_membership_one(self, val):
        if val < self.membership_param_one:
            return 0
        elif val > 2:
            return 1
        else:
            return self.one_a * val + self.one_b
    
    def defuzzify(self, val):
        mem_0 = self.get_membership_zero(val)
        mem_1 = self.get_membership_one(val)
        mem_half = self.get_membership_half(val)
        idx = np.argmax([mem_0, mem_half, mem_1])
        if idx == 0:
            return 1
        elif idx == 1:
            return 1.5
        else:
            return 2
        

    def get_membership_half(self, val):
        if val < self.membership_param_half[0]:
            return 0
        elif val > self.membership_param_half[1]:
            return 0
        elif val >= self.membership_param_half[0] and val < 1.5:
            return self.left_half_a * val + self.left_half_b
        else:
            return self.right_half_a * val + self.right_half_b
    
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
                sck.close()
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
        #print(f"{self.idx} exited its listener!")
    
    def poller(self):
        #while not self.decided:
        while True:
            if num_decided == num_individuals:
                break
            if not self.phase_one_is_finished[self.query_rnd]:
                #print(f"{self.idx} --- rnd_opinion is {self.rnd_opinions[self.query_rnd - 1]}")
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
                            if self.decided:
                                success = send_msg(indiv, "1:" + str(self.query_rnd) + ":" + str(self.opinion + 1))
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
                    self.phase_two_value.append(1.5)
                #print(f"{self.idx} finished phase one of round {self.query_rnd}")
            if self.phase == 2:
                #print(f"{self.idx} started phase two of round {self.query_rnd}")
                #print(f"{self.idx} phase two value in round {self.query_rnd} is {self.phase_two_value[self.query_rnd - 1]}")
                indiv_sample = sample_individuals(self.idx)
                while not len(indiv_sample) == 0:
                    if num_decided == num_individuals:
                        break
                    successful_queries = []
                    for indiv in indiv_sample:
                        with self.opinion_lock:
                            if self.decided:
                                success = send_msg(indiv, "2:" + str(self.query_rnd) + ":" + str(self.get_membership_zero(self.opinion + 1)) + ":" + str(self.get_membership_half(self.opinion + 1)) + ":" + str(self.get_membership_one(self.opinion + 1)))
                            else:
                                success = send_msg(indiv, "2:" + str(self.query_rnd) + ":" + str(self.phase_two_membership_zero[self.query_rnd - 1]) + ":" + str(self.phase_two_membership_half[self.query_rnd - 1]) + ":" + str(self.phase_two_membership_one[self.query_rnd - 1]))
                        if success:
                            successful_queries.append(indiv)
                    for indiv in successful_queries:
                        indiv_sample.remove(indiv)
                self.phase = 1
                if self.decided:
                    self.phase_one_is_finished[self.query_rnd] = False
                #print(f"{self.idx} finished phase two of round {self.query_rnd}")
        #print(f"{self.idx} exited its poller!")
    
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
                    self.phase_two_value.append(1.5)
                
                while len(self.phase_two_membership_zero) < msg_rnd:
                    self.phase_two_membership_zero.append(0)
                
                while len(self.phase_two_membership_half) < msg_rnd:
                    self.phase_two_membership_half.append(0)
                
                while len(self.phase_two_membership_one) < msg_rnd:
                    self.phase_two_membership_one.append(0)
                
                if not self.ignore_phase_one_messages[msg_rnd - 1] and not self.decided:
                    self.first_phase_cntr[msg_rnd - 1] = self.first_phase_cntr[msg_rnd - 1] + 1
                    self.phase_one_messages[msg_rnd - 1].append(float(splitted_msg[2]))
                    if self.first_phase_cntr[msg_rnd - 1] >= num_individuals - t:
                        self.ignore_phase_one_messages[msg_rnd - 1] = True
                        #one_cntr = 0
                        #zero_cntr = 0
                        #for val in self.phase_one_messages[msg_rnd - 1]:
                        #    if val == 0:
                        #        zero_cntr += 1
                        #    elif val == 1:
                        #        one_cntr += 1
                        
                        self.phase_two_value[msg_rnd - 1] = np.mean(self.phase_one_messages[msg_rnd - 1])

                        self.phase_two_membership_zero[msg_rnd - 1] = self.get_membership_zero(self.phase_two_value[msg_rnd - 1])
                        self.phase_two_membership_one[msg_rnd - 1] = self.get_membership_one(self.phase_two_value[msg_rnd - 1])
                        self.phase_two_membership_half[msg_rnd - 1] = self.get_membership_half(self.phase_two_value[msg_rnd - 1])
                        
                        #print(f"{self.idx} --- phase one - phase one messages are {self.phase_one_messages[msg_rnd - 1]}")
                        #print(f"{self.idx} --- phase one - zero_cntr is {zero_cntr}")
                        #print(f"{self.idx} --- phase one - one_cntr is {one_cntr}")
                        
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
                
                while len(self.agg_opinions) < msg_rnd:
                    self.agg_opinions.append(0)
                ####### The <= is very important! self.rnd_opinions starts with a value in
                ####### the first element, and is not initially empty.
                while len(self.rnd_opinions) <= msg_rnd:
                    if self.decided:
                        self.rnd_opinions.append(self.opinion)
                    else:
                        self.rnd_opinions.append(-1)
                ####### The <= is necessary because it is setting a value for the future, not this round.
                with self.phase_one_is_finished_lock:
                    while len(self.phase_one_is_finished) <= msg_rnd:
                        self.phase_one_is_finished.append(True)
                
                if not self.ignore_phase_two_messages[msg_rnd - 1] and not self.decided:
                    self.second_phase_cntr[msg_rnd - 1] += 1
                    self.phase_two_messages[msg_rnd - 1].append((float(splitted_msg[2]), float(splitted_msg[3]), float(splitted_msg[4])))
                    if self.second_phase_cntr[msg_rnd - 1] >= num_individuals - t:
                        #print(f"{self.idx} entered phase 2.")
                        self.ignore_phase_two_messages[msg_rnd - 1] = True
                        weights = 0
                        for mems in self.phase_two_messages[msg_rnd - 1]:
                            self.agg_opinions[msg_rnd - 1] += mems[0] + 1.5 * mems[1] + 2 * mems[2]
                            weights += mems[0] + mems[1] + mems[2]
                        self.agg_opinions[msg_rnd - 1] = self.agg_opinions[msg_rnd - 1] / weights
                        #print(f"{self.idx} --- phase two - zero_cntr is {zero_cntr}")
                        #print(f"{self.idx} --- phase two - one_cntr is {one_cntr}")
                        #print(f"{self.idx} --- phase two - phase two messages are {self.phase_two_messages[msg_rnd-1]}")
                            
                        ###### VERY IMPORTANT: Is it necessary to defuzzify the value? TODO:
                        #self.rnd_opinions[msg_rnd] = self.agg_opinions[msg_rnd - 1]
                        #self.rnd_opinions[msg_rnd] = self.defuzzify(self.agg_opinions[msg_rnd - 1])
                        global num_decided
                        if self.defuzzify(self.phase_two_value[msg_rnd - 1]) == self.defuzzify(self.agg_opinions[msg_rnd - 1]) and not self.defuzzify(self.agg_opinions[msg_rnd - 1]) == 1.5:
                            #print(f"{self.idx} is in case 1 in round {self.query_rnd}")
                            self.rnd_opinions[msg_rnd] = self.defuzzify(self.agg_opinions[msg_rnd - 1])
                                
                            if self.query_rnd == msg_rnd:
                                self.opinion = self.defuzzify(self.agg_opinions[msg_rnd - 1]) - 1
                                self.decided = True
                                with num_decided_lock:
                                    num_decided += 1
                                print(f"Individual {self.idx} decided {self.opinion} - Round opinions are {self.rnd_opinions} - phase two values are {self.phase_two_value} - aggregated values are {self.agg_opinions}")
                            
                            with self.phase_one_is_finished_lock:
                                #self.phase_one_is_finished[msg_rnd] = True
                                self.phase_one_is_finished[msg_rnd] = False

                        elif self.defuzzify(self.phase_two_value[msg_rnd - 1]) == self.defuzzify(self.agg_opinions[msg_rnd - 1]) and self.defuzzify(self.agg_opinions[msg_rnd - 1]) == 1.5:
                            #print(f"{self.idx} is in case 2 in round {self.query_rnd}")
                            self.rnd_opinions[msg_rnd] = self.defuzzify(self.agg_opinions[msg_rnd - 1] + uniform(-0.5, 0.5))
                            with self.phase_one_is_finished_lock:
                                self.phase_one_is_finished[msg_rnd] = False
                        
                        elif self.defuzzify(self.phase_two_value[msg_rnd - 1]) == 1.5 and not self.defuzzify(self.agg_opinions[msg_rnd - 1]) == 1.5:
                            #print(f"{self.idx} is in case 3 in round {self.query_rnd}")
                            self.rnd_opinions[msg_rnd] = self.defuzzify(self.agg_opinions[msg_rnd - 1])
                            with self.phase_one_is_finished_lock:
                                self.phase_one_is_finished[msg_rnd] = False
                        
                        elif (not self.defuzzify(self.phase_two_value[msg_rnd - 1]) == 1.5) and self.defuzzify(self.agg_opinions[msg_rnd - 1]) == 1.5:
                            #print(f"{self.idx} is in case 4 in round {self.query_rnd}")
                            self.rnd_opinions[msg_rnd] = self.defuzzify(self.agg_opinions[msg_rnd - 1] + uniform(-0.5, 0.5))
                            with self.phase_one_is_finished_lock:
                                self.phase_one_is_finished[msg_rnd] = False
                        
                        else:
                            #print(f"{self.idx} is in case 5 in round {self.query_rnd}")
                            self.rnd_opinions[msg_rnd] = self.defuzzify(self.agg_opinions[msg_rnd - 1])
                            with self.phase_one_is_finished_lock:
                                self.phase_one_is_finished[msg_rnd] = False
                                




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



num_individuals = 11
t = 2
port_base = 5000
ex_thr = 5
alpha = 1
beta = 2
k = 3
individual_ports = []
individual_opinions = []
individual_membership_params_zero = []
individual_membership_params_half = []
individual_membership_params_one = []
individual_thresholds_zero = []
individual_thresholds_one = []
individuals = {}
threads = []

num_decided = 0
num_decided_lock = threading.Lock()

#manual_initial_opinion = [2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2]
#manual_initial_opinion = [2, 1, 2, 2, 1, 2]
open("round_opinions_Fuzzy_Ben-Or.txt", "w").close()

for i in range(num_individuals):
    individual_ports.append(port_base + i)
    #individual_opinions.append(randint(-1, 1))
    
    individual_opinions.append(randint(1, 2))
    #individual_opinions.append(manual_initial_opinion[i])
    
    individual_membership_params_zero.append(1.5)
    individual_membership_params_half.append((1.25, 1.75))
    #individual_membership_params_half.append((1.5 - 1 / (3 * num_individuals), 1.5 + 1 / (3 * num_individuals)))
    individual_membership_params_one.append(1.5)
    individual_thresholds_zero.append(1.2)
    individual_thresholds_one.append(1.8)

for i in range(num_individuals):
    indiv = individual(i, individual_opinions[i], ex_thr, individual_membership_params_zero[i], individual_membership_params_half[i], individual_membership_params_one[i], individual_thresholds_zero[i], individual_thresholds_one[i], "127.0.0.1", individual_ports[i])
    threads.append(threading.Thread(target=indiv.listener, args=()))
    threads[-1].start()
print(f"Initial opinions are {individual_opinions}")
while num_decided < num_individuals:
    continue
print("Everyone decided!")