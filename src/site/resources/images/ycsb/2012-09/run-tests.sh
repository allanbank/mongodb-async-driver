#! /bin/bash

###
# #%L
# run-tests.sh - mongodb-async-driver - Allanbank Consulting, Inc.
# %%
# Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
# %%
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# #L%
###

connections=1
for threads in 1 2 5 10 20 25; do
#  for workload in workloada workloadb workloadc workloadd workloade workloadf ; do
  for workload in workloade ; do
    for driver in mongodb mongodb-async; do
      mongo --eval "db.usertable.remove();db.runCommand({'compact':'usertable','force':true});" 192.168.1.15:27017/ycsb
      ./bin/ycsb load ${driver} -s -P workloads/${workload} -threads ${threads}            \
                 -p mongodb.url=mongodb://192.168.1.15:27017                               \
                 -p mongodb.maxconnections=${connections}                                  \
                 >  ${driver}-${workload}-threads_${threads}-conns_${connections}-load.out \
                 2> ${driver}-${workload}-threads_${threads}-conns_${connections}-load.err
      ./bin/ycsb run ${driver} -s -P workloads/${workload} -threads ${threads}             \
                 -p mongodb.url=mongodb://192.168.1.15:27017                               \
                 -p mongodb.maxconnections=${connections}                                  \
                  >  ${driver}-${workload}-threads_${threads}-conns_${connections}-run.out \
                  2> ${driver}-${workload}-threads_${threads}-conns_${connections}-run.err
    done
  done
done



connections=2
for threads in 2 5 10 15 20 25 50; do
#  for workload in workloada workloadb workloadc workloadd workloade workloadf ; do
  for workload in workloade ; do
    for driver in mongodb mongodb-async; do
      mongo --eval "db.usertable.remove();db.runCommand({'compact':'usertable','force':true});" 192.168.1.15:27017/ycsb
      ./bin/ycsb load ${driver} -s -P workloads/${workload} -threads ${threads}            \
                 -p mongodb.url=mongodb://192.168.1.15:27017                               \
                 -p mongodb.maxconnections=${connections}                                  \
                 >  ${driver}-${workload}-threads_${threads}-conns_${connections}-load.out \
                 2> ${driver}-${workload}-threads_${threads}-conns_${connections}-load.err
      ./bin/ycsb run ${driver} -s -P workloads/${workload} -threads ${threads}             \
                 -p mongodb.url=mongodb://192.168.1.15:27017                               \
                 -p mongodb.maxconnections=${connections}                                  \
                  >  ${driver}-${workload}-threads_${threads}-conns_${connections}-run.out \
                  2> ${driver}-${workload}-threads_${threads}-conns_${connections}-run.err
    done
  done
done


connections=10
for threads in 10 15 20 30 40 50 75 100 125; do
#  for workload in workloada workloadb workloadc workloadd workloade workloadf ; do
  for workload in workloade ; do
    for driver in mongodb mongodb-async; do
      mongo --eval "db.usertable.remove();db.runCommand({'compact':'usertable','force':true});" 192.168.1.15:27017/ycsb
      ./bin/ycsb load ${driver} -s -P workloads/${workload} -threads ${threads}            \
                 -p mongodb.url=mongodb://192.168.1.15:27017                               \
                 -p mongodb.maxconnections=${connections}                                  \
                 >  ${driver}-${workload}-threads_${threads}-conns_${connections}-load.out \
                 2> ${driver}-${workload}-threads_${threads}-conns_${connections}-load.err
      ./bin/ycsb run ${driver} -s -P workloads/${workload} -threads ${threads}             \
                 -p mongodb.url=mongodb://192.168.1.15:27017                               \
                 -p mongodb.maxconnections=${connections}                                  \
                  >  ${driver}-${workload}-threads_${threads}-conns_${connections}-run.out \
                  2> ${driver}-${workload}-threads_${threads}-conns_${connections}-run.err
    done
  done
done
