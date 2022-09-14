#include <iostream>
#include "./src/storage/storage.h"

int main() {
    storageManager stmgr;
    stmgr.addPage();
    const char *tmp = "hello world";
    char *dst = (char *) malloc(8192);
    stmgr.writePage(0, (void *) tmp);
    stmgr.readPage(0, (void *) dst);
    std::cout << dst;
    return 0;
}
