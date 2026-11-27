#! /usr/bin/python3

import sys
import os
import time
import random
from multiprocessing import Process, Lock, Condition, Value, Array

class Road:
    def __init__(self):
        self.lock = Lock()
        self.road = Condition(self.lock)
        self.cur_dir = Value('i',-1,lock=False)
        self.nb_used = Value('i',0,lock=False)
        
    def enter_road(self, direction):
        with self.lock:
            while self.cur_dir.value != direction and self.cur_dir.value != -1:
                self.road.wait()
                self.road.notify()
            self.cur_dir.value = direction
            self.nb_used.value += 1
        
    def exit_road(self):
        with self.lock:
            self.nb_used.value -= 1
            if self.nb_used.value == 0 :
                self.cur_dir.value = -1
                self.road.notify()

def drive(road_type, direction, identifier):
    print("Vehicule %d, coming from %d goes through the %s" % (identifier, direction, road_type))
    time.sleep(random.random())

def vehicule(nb_times, direction, road):
    identifier = os.getpid()
    random.seed(identifier)
    for i in range(nb_times):
        drive("Double road", direction, identifier)
        road.enter_road(direction)
        print("Vehicule %d, coming from %d enters the small road" % (identifier, direction))
        drive("Small road", direction, identifier)
        road.exit_road()
        print("Vehicule %d, coming from %d exits the small road" % (identifier, direction))
    print("Vehicule %d, coming from %d finishes" % (identifier, direction))
        
if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Usage : %s <Nb vehicules sens O> <Nb vehicules sens 1> <Nb passages sur VU>" % sys.argv[0]);
        sys.exit(1)

    nb_vehicules = [int(sys.argv[1]), int(sys.argv[2])]
    nb_times = int(sys.argv[3])
    
    road = Road()

    processes = []
    for _ in range(nb_vehicules[0]):
        v0 = Process(target=vehicule, args=(nb_times, 0, road))
        v0.start()
        processes.append(v0)

    for _ in range(nb_vehicules[1]):
        v1 = Process(target=vehicule, args=(nb_times, 1, road))
        v1.start()
        processes.append(v1)

    for process in processes:
        process.join()
