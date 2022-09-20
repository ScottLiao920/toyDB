#include <iostream>
#include "./src/storage/storage.h"
#include "./src/bufferpool/bufferpool.h"
#include "./src/parse/parser.h"

int main() {
    storageManager stmgr;
    stmgr.addPage();
    bufferPoolManager bpmgr(&stmgr);
    const char *tmp = "hello world";
    char *dst = (char *) malloc(8192);
    stmgr.writePage(0, (void *) tmp);
//    for (auto i = 0; i < 12; i++) {
//        bpmgr.readFromDisk(0);
//    }
    for (auto i = 0; i < 10; i++)
        bpmgr.insertToFrame(0, tmp, 12);
    stmgr.readPage(0, (void *) dst);
    for (auto i = 0; i < BUFFER_POOL_SIZE; i++) {
        bpmgr.printContent(i);
    }
//    bpmgr.writeToDisk(1, 0);
    std::cout << dst;
    parser p;
    p.parse("SELECT SUM(*) as fck, col1 from table1, table2 where table1.id = table2.id and tabel2.col2 < 100");
    return 0;
}
