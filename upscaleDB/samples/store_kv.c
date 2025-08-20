#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ups/upscaledb.h>

void error(const char *foo, ups_status_t st) {
    printf("%s() returned error %d: %s\n", foo, st, ups_strerror(st));
    exit(-1);
}

int main(int argc, char **argv) {
    if (argc != 3) {
        printf("Usage: %s <key> <value>\n", argv[0]);
        return 1;
    }

    ups_status_t st;
    ups_env_t *env;
    ups_db_t *db;
    ups_key_t key = {0};
    ups_record_t record = {0};

    // Try to open existing database first
    st = ups_env_open(&env, "store.db", 0, 0);
    if (st == UPS_FILE_NOT_FOUND) {
        // If it doesn't exist, create it
        st = ups_env_create(&env, "store.db", 0, 0664, 0);
        if (st != UPS_SUCCESS)
            error("ups_env_create", st);
        
        st = ups_env_create_db(env, &db, 1, 0, 0);
        if (st != UPS_SUCCESS)
            error("ups_env_create_db", st);
    }
    else if (st == UPS_SUCCESS) {
        // Open existing database
        st = ups_env_open_db(env, &db, 1, 0, 0);
        if (st != UPS_SUCCESS)
            error("ups_env_open_db", st);
    }
    else {
        error("ups_env_open", st);
    }

    // Set up the key and value from command line arguments
    key.data = argv[1];
    key.size = strlen(argv[1]) + 1;
    record.data = argv[2];
    record.size = strlen(argv[2]) + 1;

    // Store the key-value pair
    st = ups_db_insert(db, 0, &key, &record, UPS_OVERWRITE);
    if (st != UPS_SUCCESS)
        error("ups_db_insert", st);

    printf("Successfully stored: %s -> %s\n", (char*)key.data, (char*)record.data);

    // Verify by reading it back
    ups_record_t verify = {0};
    st = ups_db_find(db, 0, &key, &verify, 0);
    if (st != UPS_SUCCESS)
        error("ups_db_find", st);

    printf("Verified value: %s\n", (char*)verify.data);

    // Cleanup
    st = ups_env_close(env, UPS_AUTO_CLEANUP);
    if (st != UPS_SUCCESS)
        error("ups_env_close", st);

    return 0;
}

