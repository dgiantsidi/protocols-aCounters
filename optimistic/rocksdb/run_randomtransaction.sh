#!/bin/bash


for num in `seq 1000 3000 100000`; do
  for threads in `seq 1 7 50`; do
    echo " **** $num $threads **** "
    ./db_bench --benchmarks=randomtransaction --num=$num --transaction_db=true --threads=$threads --transaction_sets=5 2>&1 | tail -n 2
  done
done
