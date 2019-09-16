#include <string.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "../random/random.h"
#include "operation_sequence_writer.h"

#define FALSE 0
#define TRUE 1
#define FAILURE 0
#define SUCCESS 1
#define READ 0
#define WRITE 1
#define DELETE 2

const char * directory = "../../misc_phd/input/operation_sequences";

void write_operation_sequence(char comment[200], int feedback_enabled, int indexes_enabled, int64_t max_key, int64_t capacity, int64_t max_load,
        int deletions_enabled, int64_t num_operations, int * operation, int64_t * key, int * expected_result, char fname[200],
        int64_t * index, double percentage_reads, double percentage_repeat_reads, int64_t min_read_streak_length, int64_t max_read_streak_length){
  //printf("write %s\n", fname);
  FILE * fp = fopen(fname, "w");
  int64_t g;
  fprintf(fp, "%s\n", comment);
  fprintf(fp, "HAS_FEEDBACK %d\n", feedback_enabled);
  fprintf(fp, "HAS_INDEXES %d\n", indexes_enabled);
  fprintf(fp, "DELETIONS_ENABLED %d\n", deletions_enabled);
  fprintf(fp, "MAX_KEY %ld\n", max_key);
  fprintf(fp, "CAPACITY %ld\n", capacity);
  fprintf(fp, "MAX_LOAD %ld\n", max_load);
  fprintf(fp, "PERCENTAGE_READS %f\n", percentage_reads);
  fprintf(fp, "PERCENTAGE_REPEAT_READS %f\n", percentage_repeat_reads);
  fprintf(fp, "MAX_READ_STREAK_LENGTH %ld\n", max_read_streak_length);
  fprintf(fp, "MIN_READ_STREAK_LENGTH %ld\n", min_read_streak_length);
  fprintf(fp, "NUM_OPERATIONS %ld\n", num_operations);
  if(feedback_enabled){
    if(indexes_enabled){
      fprintf(stderr, "indexes not implemented yet!\n");
      exit(1);
      for(g = 0; g < num_operations; g++){
        fprintf(fp, "%d %ld %d %ld\n", operation[g], key[g], expected_result[g], index[g]);
      }
    }else{
      for(g = 0; g < num_operations; g++){
        fprintf(fp, "%d %ld %d\n", operation[g], key[g], expected_result[g]);
      }
    }
  }else{
    //printf("writing operation key\n");
    for(g = 0; g < num_operations; g++){
      fprintf(fp, "%d %ld\n", operation[g], key[g]);
    }
  }
  fclose(fp);
}

void generate_special_operation_sequence_without_feedback_001(){
  char fname[200], comment[200];
  int * operation;
  int64_t g, h, j, * key, * saved_key, num_reads, num_writes, num_deletes;
  double percentage_reads, percentage_repeat_reads, min_read_streak_length, max_read_streak_length;
  int64_t num_operations, max_key, deletions_enabled;
  int64_t first_threshold, second_threshold, num_phase_two_reads;
  num_reads = num_writes = num_deletes = 0;
  percentage_reads = percentage_repeat_reads = min_read_streak_length = max_read_streak_length = 0.0;
  first_threshold = 9216;
  num_phase_two_reads = 32;
  second_threshold = 9728;
  num_operations = 10240;
  max_key = 100000;
  deletions_enabled = FALSE;
  strcpy(comment, " ");
  sprintf(fname, "%s/without_feedback/without_deletions/special_operation_sequence_without_feedback-004-%ld-%ld",
          directory, num_operations, max_key);
  operation = calloc(num_operations, sizeof (int));
  if(operation == NULL){
    fprintf(stderr, "Couldn't get memory for operation\n");
    exit(1);
  }
  key = calloc(num_operations, sizeof (int64_t));
  if(key == NULL){
    fprintf(stderr, "Couldn't get memory for key\n");
    exit(1);
  }
  saved_key = calloc(num_operations, sizeof (int64_t));
  if(saved_key == NULL){
    fprintf(stderr, "Couldn't get memory for saved_key\n");
    exit(1);
  }
  g = 0; // write 2n log n items
  while(g < first_threshold){
    operation[g] = WRITE;
    key[g] = g + 1;
    g++;
  }
  h = 0; // read in order oldest to newest the last 2(ka-1)n items)
  while(g < second_threshold){
    operation[g] = READ;
    key[g] = key[g - num_phase_two_reads];
    saved_key[h] = key[g];
    g++;
    h++;
  }
  for(h = 0; h < 15; h++){
    j = 0;
    while(j < num_phase_two_reads){
      operation[g] = READ;
      key[g] = saved_key[j];
      g++;
      j++;
    }
  }
  while(g < num_operations){ // write n items
    operation[g] = WRITE;
    key[g] = g + 1;
    g++;
  }
  //printf("num_reads %ld  num_writes %ld  num_deletes %ld  total %ld\n",
  //        num_reads, num_writes, num_deletes, num_reads + num_writes + num_deletes);
  write_operation_sequence(comment, 0, 0, max_key, 0, 0, deletions_enabled,
          num_operations, operation, key, NULL, fname, NULL, percentage_reads,
          percentage_repeat_reads, min_read_streak_length, max_read_streak_length);
}

void generate_operation_sequence_without_feedback(int64_t num_operations, int64_t max_key,
        int deletions_enabled, double percentage_reads, char comment[200],
        double percentage_repeat_reads, int64_t min_read_streak_length, int64_t max_read_streak_length){
  char fname[200];
  int * operation, g, h;
  int64_t * key, num_reads, num_writes, num_deletes, streak_length, repeated_key;
  double x, y;
  num_reads = num_writes = num_deletes = 0;
  //printf("without feedback\n");
  if(deletions_enabled == TRUE){
    sprintf(fname, "%s/without_feedback/deletions/operation_sequence_without_feedback-%ld-%ld",
            directory, num_operations, max_key);
    //        sprintf(fname, "../misc_phd/input/operation_sequences/without_feedback/deletions/%ld/operation_sequence_without_feedback-%ld-%ld", 
    //                num_operations, num_operations, max_key);
  }else{
    sprintf(fname, "%s/without_feedback/without_deletions/operation_sequence_without_feedback-%ld-%ld",
            directory, num_operations, max_key);
    //        sprintf(fname, "../misc_phd/input/operation_sequences/without_feedback/without_deletions/%ld/operation_sequence_without_feedback-%ld-%ld", 
    //                num_operations, num_operations, max_key);
  }
  operation = calloc(num_operations, sizeof(int));
  if(operation == NULL){
    fprintf(stderr, "Couldn't get memory for operation\n");
    exit(1);
  }
  key = calloc(num_operations, sizeof(int64_t));
  if(key == NULL){
    fprintf(stderr, "Couldn't get memory for key\n");
    exit(1);
  }
  for(g = 0; g < num_operations; g++){
    x = random_double();
    //        printf("random_double chose: %f\n", x);
    if(deletions_enabled == TRUE){
      if(x < percentage_reads){
        //                printf("choosing to read\n");
        num_reads++;
        operation[g] = 0;
      } else {
        operation[g] = rand_in_range(1, 3);
        switch(operation[g]){
          case 0:
            fprintf(stderr, "error\n");
            exit(1);
          case 1:
            num_writes++;
            break;
          case 2:
            num_deletes++;
            break;
          default:
            fprintf(stderr, "error\n");
            exit(1);
        }
        //                printf("choosing operation %d\n", operation[g]);
      }
    }else{
      if(x < percentage_reads){
        //                printf("choosing to read\n");
        num_reads++;
        operation[g] = 0;
      }else{
        //                printf("choosing to write\n");
        num_writes++;
        operation[g] = 1;
      }
    }
    key[g] = rand_in_range(1, max_key);
    if(operation[g] != 2){
      y = random_double();
      if(y < percentage_repeat_reads){
        repeated_key = key[g];
        streak_length = rand_in_range(min_read_streak_length, max_read_streak_length);
        if(num_operations < (g + streak_length - 1)){
          streak_length = num_operations - g - 1;
        }
        g++;
        for(h = 0; h < streak_length - 1; h++){
          operation[g] = 0;
          num_reads++;
          key[g] = repeated_key;
          g++;
        }
      }
    }
  }
  //printf("num_reads %ld  num_writes %ld  num_deletes %ld  total %ld\n",
  //        num_reads, num_writes, num_deletes, num_reads + num_writes + num_deletes);
  write_operation_sequence(comment, 0, 0, max_key, 0, 0, deletions_enabled,
          num_operations, operation, key, NULL, fname, NULL, percentage_reads,
          percentage_repeat_reads, min_read_streak_length, max_read_streak_length);
}

void generate_operation_sequence_with_feedback(int64_t num_operations, int64_t max_key,
        int64_t capacity, int deletions_enabled, double percentage_reads, char comment[200],
        double percentage_repeat_reads, int64_t min_read_streak_length, int64_t max_read_streak_length){
  FILE * fp;
  char fname[200];
  int * operation, * cache, * expected_result, * load, g, h;
  int64_t * key, size, max_load, num_reads, num_writes, num_deletes, streak_length, repeated_key;
  double x, y;
  size = max_load = num_reads = num_writes = num_deletes = 0;
  operation = calloc(num_operations, sizeof(int));
  key = calloc(num_operations, sizeof(int64_t));
  cache = calloc(max_key, sizeof(int));
  expected_result = calloc(num_operations, sizeof(int));
  load = calloc(num_operations, sizeof(int));
  for(g = 0; g < num_operations; g++){
    x = random_double();
    //printf("random_double chose: %f\n", x);
    if(deletions_enabled == TRUE){
      if(x < percentage_reads){
        //printf("choosing to read\n");
        num_reads++;
        operation[g] = 0;
      }else{
        operation[g] = rand_in_range(1, 3);
        switch(operation[g]){
          case 0:
            fprintf(stderr, "error\n");
            exit(1);
          case 1:
            num_writes++;
            break;
          case 2:
            num_deletes++;
            break;
          default:
            fprintf(stderr, "error\n");
            exit(1);
        }
        //printf("choosing operation %d\n", operation[g]);
      }
    }else{
      if(x < percentage_reads){
        //printf("choosing to read\n");
        num_reads++;
        operation[g] = 0;
      }else{
        //printf("choosing to write\n");
        num_writes++;
        operation[g] = 1;
      }
    }
    key[g] = rand_in_range(1, max_key);
    switch(operation[g]){
      case READ:
        if(cache[key[g]] == 0){
          expected_result[g] = 0; /* not found */
        }else{
          expected_result[g] = 1; /* found */
        }
        break;
      case WRITE:
        if(size >= capacity){
          expected_result[g] = 0; /* cache is full, therefore cannot write key */
        }else{
          if(cache[key[g]] == 0){ /* not already in cache, therefore write key */
            cache[key[g]] = 1;
            expected_result[g] = 1;
            size++;
          } else {
            expected_result[g] = 0; /* already in cache, therefore cannot write key */
          }
        }
        break;
      case DELETE:
        if(deletions_enabled){
          if(cache[key[g]] == 0){ /* key not found, therefore cannot delete key */
            //#ifdef SHOW_PROGRESS
            //printf("failed deletion\n");
            //#endif
            expected_result[g] = 0;
          }else{ /* key is in cache, therefore delete key */
            //#ifdef SHOW_PROGRESS
            //printf("succeeded deletion\n");
            //#endif
            cache[key[g]] = 0;
            expected_result[g] = 1;
            size--;
            if(size < 0){
              fprintf(stderr, "Error: cache underflow. How did the size of the cache become negative?!\n");
              exit(EXIT_FAILURE);
            }
          }
        }else{
          expected_result[g] = 0;
        }
        break;
      default:
        break;
    }
    if((operation[g] == READ && expected_result[g] == SUCCESS) || operation[g] == WRITE){
      y = random_double();
      if(y < percentage_repeat_reads){
        repeated_key = key[g];
        streak_length = rand_in_range(min_read_streak_length, max_read_streak_length);
        if(num_operations < (g + streak_length - 1)){
          streak_length = num_operations - g - 1;
        }
        g++;
        for(h = 0; h < streak_length; h++){
          operation[g] = 0;
          num_reads++;
          key[g] = repeated_key;
          if(cache[key[g]] == 0){
            expected_result[g] = 0; /* not found */
            fprintf(stderr, "A repeated read is unsuccessful. WTF?!\n");
            exit(1);
          }else{
            expected_result[g] = 1; /* found */
          }
          g++;
        }
      }
    }
    load[g] = size;
    max_load = (load[g] > max_load) ? load[g] : max_load;
  }
  if(deletions_enabled == TRUE){
    sprintf(fname, "%s/feedback/deletions/operation_sequence_with_feedback-%ld-%ld-%ld-%ld",
            directory, num_operations, max_key, capacity, max_load);
    //        sprintf(fname, "../misc_phd/input/operation_sequences/feedback/deletions/%ld/operation_sequence_with_feedback-%ld-%ld-%ld-%ld", 
    //                num_operations, num_operations, max_key, capacity, max_load);
  }else{
    sprintf(fname, "%s/feedback/without_deletions/operation_sequence_with_feedback-%ld-%ld-%ld-%ld",
            directory, num_operations, max_key, capacity, max_load);
    //        sprintf(fname, "../misc_phd/input/operation_sequences/feedback/without_deletions/%ld/operation_sequence_with_feedback-%ld-%ld-%ld-%ld", 
    //                num_operations, num_operations, max_key, capacity, max_load);
  }
  //printf("num_reads %ld  num_writes %ld  num_deletes %ld  total %ld\n",
  //        num_reads, num_writes, num_deletes, num_reads + num_writes + num_deletes);
  write_operation_sequence(comment, 1, 0, max_key, capacity, max_load, deletions_enabled, num_operations, operation,
          key, expected_result, fname, NULL, percentage_reads, percentage_repeat_reads,
          min_read_streak_length, max_read_streak_length);

}

void do_operation_sequence(int argc, char** argv){
  int64_t feedback_enabled, deletions_enabled, indexes_enabled;
  int64_t num_operations, max_key, capacity;
  int64_t min_read_streak_length, max_read_streak_length;
  char comment[200];
  //printf("write operation sequence\n");
  //generate_special_operation_sequence_without_feedback_001();
  //exit(0);
  double percentage_reads = 0.5;
  double percentage_repeat_reads = 0.0;
  min_read_streak_length = max_read_streak_length = 1;
  strcpy(comment, " ");
  int64_t i;
  for(i = 1; i < argc; i++){
    if(strcmp(argv[i], "--feedback_enabled") == 0){
      if(i + 1 < argc){
        feedback_enabled = (int) atoi(argv[++i]);
      }
    } else if(strcmp(argv[i], "--deletions_enabled") == 0){
      if(i + 1 < argc){
        deletions_enabled = (int) atoi(argv[++i]);
        //                strcpy(fname, &argv[++i][0]);
      }
    } else if(strcmp(argv[i], "--indexes_enabled") == 0){
      if(i + 1 < argc){
        indexes_enabled = (int) atoi(argv[++i]);
      }
    } else if(strcmp(argv[i], "--num_operations") == 0){
      if(i + 1 < argc){
        num_operations = (int64_t) atoi(argv[++i]);
      }
    } else if(strcmp(argv[i], "--max_key") == 0){
      if(i + 1 < argc){
        max_key = (int64_t) atoi(argv[++i]);
      }
    } else if(strcmp(argv[i], "--capacity") == 0){
      if (i + 1 < argc) {
        capacity = (int64_t) atoi(argv[++i]);
      }
    } else if(strcmp(argv[i], "-t") == 0){
      if(i + 1 < argc){
        capacity = (uint64_t) atoi(argv[++i]);
      }
    } else if(strcmp(argv[i], "--percentage_reads") == 0){
      if (i + 1 < argc){
        percentage_reads = (double) atof(argv[++i]);
        //printf("found options percentage_reads = %f\n", percentage_reads);
        if(percentage_reads < 0.0 || 1.0 < percentage_reads){
          fprintf(stderr, "percentage_reads out of bounds. Reset to 0.5\n");
          percentage_reads = 0.5;
        }
      }
    } else if(strcmp(argv[i], "--comment") == 0){
      if(i + 1 < argc){
        strcpy(comment, argv[++i]);
      }
    } else if(strcmp(argv[i], "--percentage_repeat_reads") == 0){
      if(i + 1 < argc){
        percentage_repeat_reads = (double) atof(argv[++i]);
        //printf("found option percentage_repeat_reads = %f\n", percentage_repeat_reads);
        if(percentage_repeat_reads < 0.0 || 1.0 < percentage_repeat_reads){
          fprintf(stderr, "percentage_repeat_reads out of bounds. Reset to 0.0\n");
          percentage_repeat_reads = 0.0;
        }
      }
    } else if(strcmp(argv[i], "--max_read_streak_length") == 0){
      if(i + 1 < argc){
        max_read_streak_length = (int64_t) atoi(argv[++i]);
        //printf("found option max_read_streak_length = %ld\n", max_read_streak_length);
        if(max_read_streak_length < 1){
          max_read_streak_length = 1;
        }
      }
    } else if(strcmp(argv[i], "--min_read_streak_length") == 0){
      if(i + 1 < argc){
        min_read_streak_length = (int64_t) atoi(argv[++i]);
        //printf("found option min_read_streak_length = %ld\n", min_read_streak_length);
      }
    }
  }
  if(max_read_streak_length < min_read_streak_length){
    max_read_streak_length = min_read_streak_length = 1;
  }
  if(feedback_enabled == FALSE){
    generate_operation_sequence_without_feedback(num_operations, max_key, deletions_enabled,
            percentage_reads, comment, percentage_repeat_reads, min_read_streak_length, max_read_streak_length);
  } else {
    generate_operation_sequence_with_feedback(num_operations, max_key, capacity,
            deletions_enabled, percentage_reads, comment, percentage_repeat_reads, min_read_streak_length, max_read_streak_length);
  }
}
