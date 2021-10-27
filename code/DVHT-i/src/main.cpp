#include "base_struct.hpp"

#include "source.hpp"
#include "worker.hpp"
#include "run.hpp"
#include <iostream>

int main ( int argc, char *argv[] ) {


    //int 	workerNum = hIO.getSzProc() - 1;

    int   aggNum ;
    Option::parse(argc, argv, aggNum);
    MPIIO 	hIO(argc, argv, aggNum);
    int 	workerNum = hIO.getSzProc() - 1 - aggNum;
	

    //Option::print();

    if(hIO.isMaster()) {
        Option::print();
        //cout << "workerNum = " << hIO.getWorkerNum() << endl;
    }


    run_exp(Option::inFileName, Option::outPath, hIO, workerNum, aggNum, Option::budget, Option::trial);

    hIO.cleanup(aggNum);

}
