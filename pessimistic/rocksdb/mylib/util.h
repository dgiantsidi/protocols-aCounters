#pragma once
#include <iostream>
#include <string>
#include <fstream>

namespace rocksdb {


/*static int return_walcounter_init_stable_value() {
    std::ifstream ifs;
    std::string line, prev;
    bool first = true;

    ifs.open ("wal_counter_init.txt", std::ifstream::in);
    while (std::getline(ifs, line)) {
        if (first)
            first = false;
        else
            prev = line;
    }

    std:: cout << "prev: " << prev <<"\n";
    int id =  std::stoi(prev);
    std:: cout << "prev: " << id <<"\n";
    ifs.close();

    return id;
}

static int return_timestampcounter_last_stable_value() {}
    std::ifstream ifs;
    std::string line, prev;
    bool first = true;

    ifs.open ("counter.txt", std::ifstream::in);
    while (std::getline(ifs, line)) {
        if (first)
            first = false;
        else
            prev = line;
    }

    std:: cout << "prev: " << prev <<"\n";
    int id =  std::stoi(prev);
    std:: cout << "prev: " << id <<"\n";
    ifs.close();

    return id;
}*/

}
