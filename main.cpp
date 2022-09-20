#include <iostream>
#include "./src/include/storage.h"
#include "./src/include/bufferpool.h"
#include "./src/include/parser.h"

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
    p.parse("SELECT SUM(*) as fck, col1 from table1, table2 where table1.relId_ = table2.relId_ and tabel2.col2 < 100");

    rel table1, table2;
    table1.set_name_("A");
    table2.set_name_("B");
    table1.add_column("relId_", 8);
    table2.add_column("relId_", 8);
    table2.add_column("col2", sizeof(float));
    char id[8];
    table1.update_row(&bpmgr, 0, id);
    for (auto i = 0; i < BUFFER_POOL_SIZE; i++) {
        bpmgr.printContent(i);
    }
//    column col1;

    return 0;
}
