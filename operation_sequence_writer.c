/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

#include "operation_sequence_writer.h"

#define FALSE 0
#define TRUE 1
#define READ 0
#define WRITE 1
#define DELETE 2

void write_operation_sequence( int64_t num_operations, int64_t max_key, int64_t capacity, int deletions_enabled, int indexes_enabled, int feedback_enabled ){
    
    int64_t num_operations, max_key, capacity, max_load;
    int * operation, * result;
    int64_t * key, * index;
    FILE * fp;
    char fname[200];
    
    printf("write operation sequence\n");
    
}