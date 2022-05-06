#!/bin/bash

for i in $(seq 10000 10100)
do
    listen_addr="localhost:$i";
    echo ${listen_addr};
    ./target/release/chitchat-test -h ${listen_addr} --seed localhost:10050&
done;

read
kill 0
