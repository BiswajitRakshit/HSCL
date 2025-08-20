#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ups/upscaledb.h>

void error(const char *foo, ups_status_t st) {
    printf("%s() returned error %d: %s\n", foo, st, ups_strerror(st));
    exit(-1);
}

int main() {
    ups_status_t st;
    ups_env_t *env;
    ups_db_t *db;
    ups_cursor_t *cursor;
    ups_key_t key = {0};
    ups_record_t record = {0};

    // Open the existing database
    st = ups_env_open(&env, "store.db", 0, 0);
    if (st != UPS_SUCCESS)
        error("ups_env_open", st);

    st = ups_env_open_db(env, &db, 1, 0, 0);
    if (st != UPS_SUCCESS)
        error("ups_env_open_db", st);

    // Create a cursor
    st = ups_cursor_create(&cursor, db, 0, 0);
    if (st != UPS_SUCCESS)
        error("ups_cursor_create", st);

    printf("Stored key-value pairs:\n");
    printf("----------------------\n");

    // Iterate through all items
    while (1) {
        st = ups_cursor_move(cursor, &key, &record, UPS_CURSOR_NEXT);
        if (st == UPS_KEY_NOT_FOUND)  // End of database
            break;
        if (st != UPS_SUCCESS)
            error("ups_cursor_move", st);

        printf("%.*s -> %.*s\n", 
            (int)key.size - 1, (char*)key.data,    // -1 to not print null terminator
            (int)record.size - 1, (char*)record.data);
    }

    // Cleanup
    st = ups_cursor_close(cursor);
    if (st != UPS_SUCCESS)
        error("ups_cursor_close", st);

    st = ups_env_close(env, UPS_AUTO_CLEANUP);
    if (st != UPS_SUCCESS)
        error("ups_env_close", st);

    return 0;
}

