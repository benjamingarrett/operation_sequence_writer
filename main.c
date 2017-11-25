/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   main.c
 * Author: benjamin
 *
 * Created on April 30, 2017, 9:39 PM
 */

#include "operation_sequence_writer.h"

int main(int argc, char** argv) {

  //    int64_t num_operations, max_key, capacity;
  //    int deletions_enabled, indexes_enabled, feedback_enabled;
  //    
  //    num_operations = 10;
  //    max_key = 20;
  //    capacity = 30;
  //    deletions_enabled = 1;
  //    indexes_enabled = 0;
  //    feedback_enabled = 1;

  //    do_operation_sequence( num_operations, max_key, capacity, deletions_enabled, feedback_enabled, indexes_enabled );
  do_operation_sequence(argc, argv);
  return (EXIT_SUCCESS);
}

