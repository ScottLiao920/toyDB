#include <iostream>
#include "./src/storage/storage.h"
#include "./src/bufferpool/bufferpool.h"

int main() {
    storageManager stmgr;
    stmgr.addPage();
    bufferPoolManager bpmgr(&stmgr);
    const char *tmp = "hello world";
    char *dst = (char *) malloc(8192);
    stmgr.writePage(0, (void *) tmp);
    for (auto i = 0; i < 120; i++) {
        bpmgr.readFromDisk(1);
    }
    stmgr.readPage(0, (void *) dst);
    std::cout << dst;
    return 0;
}
