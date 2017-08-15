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

const char * directory = "../../misc_phd/input/operation_sequences";

void write_operation_sequence(int feedback_enabled, int indexes_enabled, int64_t max_key, int64_t capacity, int64_t max_load, 
        int deletions_enabled, int64_t num_operations, int * operation, int64_t * key, int * expected_result, char fname[200], int64_t * index){
    
    FILE * fp;
    int64_t g;
    
    printf("write %s\n", fname);
    fp = fopen(fname, "w");
    fprintf(fp, "HAS_FEEDBACK %d\n", feedback_enabled);
    fprintf(fp, "HAS_INDEXES %d\n", indexes_enabled);
    fprintf(fp, "DELETIONS_ENABLED %d\n", deletions_enabled);
    fprintf(fp, "MAX_KEY %ld\n", max_key);
    fprintf(fp, "CAPACITY %ld\n", capacity);
    fprintf(fp, "MAX_LOAD %ld\n", max_load);
    fprintf(fp, "NUM_OPERATIONS %ld\n", num_operations);
    if(feedback_enabled){
        if(indexes_enabled){
            fprintf(stderr, "indexes not implemented yet!\n");
            exit(EXIT_FAILURE);
            for(g=0; g<num_operations; g++){
                fprintf(fp, "%d %ld %d %ld\n", operation[g], key[g], expected_result[g], index[g]);
            }
        } else {
            for(g=0; g<num_operations; g++){
                fprintf(fp, "%d %ld %d\n", operation[g], key[g], expected_result[g]);
            }
        }
    } else {
        printf("writing operation key\n");
        for(g=0; g<num_operations; g++){
            fprintf(fp, "%d %ld\n", operation[g], key[g]);
        }
    }
    fclose(fp);
}

void generate_operation_sequence_without_feedback( int64_t num_operations, int64_t max_key, int deletions_enabled ){
    
    char fname[200];
    int * operation, g;
    int64_t * key;
    
    printf("without feedback\n");
    if( deletions_enabled==TRUE ){
        sprintf(fname, "%s/without_feedback/deletions/operation_sequence_without_feedback-%ld-%ld", 
                directory, num_operations, max_key);
//        sprintf(fname, "../misc_phd/input/operation_sequences/without_feedback/deletions/%ld/operation_sequence_without_feedback-%ld-%ld", 
//                num_operations, num_operations, max_key);
    } else {
        sprintf(fname, "%s/without_feedback/without_deletions/operation_sequence_without_feedback-%ld-%ld", 
                directory, num_operations, max_key);
//        sprintf(fname, "../misc_phd/input/operation_sequences/without_feedback/without_deletions/%ld/operation_sequence_without_feedback-%ld-%ld", 
//                num_operations, num_operations, max_key);
    }
    operation = calloc(num_operations, sizeof(int));
    key = calloc(num_operations, sizeof(int64_t));
    for(g=0; g<num_operations; g++){
        if( deletions_enabled==TRUE ){
            operation[g] = rand_in_range(0,3);
        } else {
            operation[g] = rand_in_range(0,2);
        }
        key[g] = rand_in_range(1,max_key);
    }
    write_operation_sequence(0, 0, max_key, 0, 0, deletions_enabled, num_operations, operation, key, NULL, fname, NULL);
}

void generate_operation_sequence_with_feedback( int64_t num_operations, int64_t max_key, int64_t capacity, int deletions_enabled ){
    
    FILE * fp;
    char fname[200];
    int * operation, * cache, * expected_result, * load, g;
    int64_t * key, size, max_load;
    
    size = max_load = 0;
    operation = calloc(num_operations, sizeof(int));
    key = calloc(num_operations, sizeof(int64_t));
    cache = calloc(max_key, sizeof(int));
    expected_result = calloc(num_operations, sizeof(int));
    load = calloc(num_operations, sizeof(int));
    for(g=0; g<num_operations; g++){
        if( deletions_enabled==TRUE ){
            operation[g] = rand_in_range(0,3);
        } else {
            operation[g] = rand_in_range(0,2);
        }
        key[g] = rand_in_range(1,max_key);
        switch(operation[g]){
            case READ:
                if(cache[key[g]]==0){
                    expected_result[g] = 0; /* not found */
                } else {
                    expected_result[g] = 1; /* found */
                }
                break;
            case WRITE:
                if(size>=capacity){
                    expected_result[g] = 0; /* cache is full, therefore cannot write key */
                } else {
                    if(cache[key[g]]==0){ /* not already in cache, therefore write key */
                        cache[key[g]]=1;
                        expected_result[g] = 1;
                        size++;
                    } else {
                        expected_result[g] = 0; /* already in cache, therefore cannot write key */
                    }
                }
                break;
            case DELETE:
                if(deletions_enabled){
                    if(cache[key[g]]==0){ /* key not found, therefore cannot delete key */
                        #ifdef SHOW_PROGRESS
                            printf("failed deletion\n");
                        #endif
                        expected_result[g] = 0;
                    } else { /* key is in cache, therefore delete key */
                        #ifdef SHOW_PROGRESS
                            printf("succeeded deletion\n");
                        #endif
                        cache[key[g]] = 0;
                        expected_result[g] = 1;
                        size--;
                        if(size<0){
                            fprintf(stderr, "Error: cache underflow. How did the size of the cache become negative?!\n");
                            exit(EXIT_FAILURE);
                        }
                    }
                } else {
                    expected_result[g] = 0;
                }
                break;
            default:
                break;
        }
        load[g] = size;
        max_load = (load[g] > max_load) ? load[g] : max_load;
    }
    if( deletions_enabled==TRUE ){
        sprintf(fname, "%s/feedback/deletions/operation_sequence_with_feedback-%ld-%ld-%ld-%ld", 
                directory, num_operations, max_key, capacity, max_load);
//        sprintf(fname, "../misc_phd/input/operation_sequences/feedback/deletions/%ld/operation_sequence_with_feedback-%ld-%ld-%ld-%ld", 
//                num_operations, num_operations, max_key, capacity, max_load);
    } else {
        sprintf(fname, "%s/feedback/without_deletions/operation_sequence_with_feedback-%ld-%ld-%ld-%ld", 
                directory, num_operations, max_key, capacity, max_load);
//        sprintf(fname, "../misc_phd/input/operation_sequences/feedback/without_deletions/%ld/operation_sequence_with_feedback-%ld-%ld-%ld-%ld", 
//                num_operations, num_operations, max_key, capacity, max_load);
    }
    write_operation_sequence(1, 0, max_key, capacity, max_load, deletions_enabled, num_operations, operation, key, expected_result, fname, NULL);

}

void do_operation_sequence( int argc, char** argv){
//void do_operation_sequence( int64_t num_operations, int64_t max_key, int64_t capacity, int deletions_enabled, int feedback_enabled, int indexes_enabled ){
    
    int i, feedback_enabled, deletions_enabled, indexes_enabled;
    int64_t num_operations, max_key, capacity;
    
    printf("write operation sequence\n");
    for(i=1; i<argc; i++){
        if(strcmp(argv[i], "--feedback_enabled")==0){
            if(i+1 < argc){
                feedback_enabled = (int)atoi(argv[++i]);
            }
        } else if(strcmp(argv[i], "--deletions_enabled")==0){
            if(i+1 < argc){
                deletions_enabled = (int)atoi(argv[++i]);
//                strcpy(fname, &argv[++i][0]);
            }
        } else if(strcmp(argv[i], "--indexes_enabled")==0){
            if(i+1 < argc){
                indexes_enabled = (int)atoi(argv[++i]);
            }
        } else if(strcmp(argv[i], "--num_operations")==0){
            if(i+1 < argc){
                num_operations = (int64_t)atoi(argv[++i]);
            }
        } else if(strcmp(argv[i], "--max_key")==0){
            if(i+1 < argc){
                max_key = (int64_t)atoi(argv[++i]);
            }
        } else if(strcmp(argv[i], "--capacity")==0){
            if(i+1 < argc){
                capacity = (int64_t)atoi(argv[++i]);
            }
        } else if(strcmp(argv[i], "-t")==0){
            if(i+1 < argc){
                capacity = (uint64_t)atoi(argv[++i]);
            }
        }
    }
    if( feedback_enabled==FALSE ){
        generate_operation_sequence_without_feedback( num_operations, max_key, deletions_enabled );
    } else {
        generate_operation_sequence_with_feedback( num_operations, max_key, capacity, deletions_enabled );
    }
}