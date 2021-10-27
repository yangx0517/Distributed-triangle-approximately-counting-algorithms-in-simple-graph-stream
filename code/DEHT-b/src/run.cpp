#include "run.hpp"

double run_mpi(const char* filename, MPIIO &hIO, int workerNum, int memSize, int lenBuf, double tolerance, unsigned int seed, std::vector<float> & oLocalCnt, double &srcCompCost, double &workerCompCostMax, double &workerCompCostSum)
{

    //Computational Cost
    clock_t begin = clock();

    hIO.init(lenBuf, workerNum);

    // Sourcce init
    if (hIO.isMaster())
    {
        Source source;
        Edge edge;
        MID dst1(0);
        MID dst2(0);
        EdgeParser parser(filename);

        while (parser.getEdge(edge)) // Stream edges
        {

            if(edge.src != edge.dst) {
                source.processEdge(edge, dst1, dst2);

				dst1 = rand() % workerNum; 
                //cout << "rand1:" << dst1 << endl;
                //hIO.bCastEdge(edge, dst1);   //单播 
                hIO.sendEdge(edge, dst1);
 
                //hIO.unCastEdge(edge, dst1);
            }
        }

        hIO.sendEndSignal();

        //std::cout << "Master: " << double(clock() - begin) / CLOCKS_PER_SEC << "\t" << hIO.getIOCPUTime() / CLOCKS_PER_SEC << "\t" <<  srcCompCost << endl;

        // Gather results from curWorkers
        double globalCnt = 0;

        // communication cost for gather
        hIO.recvCnt(source.getMaxVId(), globalCnt, oLocalCnt);

        //std::cout << source.getMaxVId() << "\t" << globalCnt << "\t" << oLocalCnt.size();

        hIO.recvTime(workerCompCostMax, workerCompCostSum);


        globalCnt = globalCnt * (workerNum * workerNum);
        for(auto it = oLocalCnt.begin(); it != oLocalCnt.end(); ++it)
        {
            *it  = *it * (workerNum * workerNum);
        }
        
        cout << "globalCnt:" << globalCnt << endl;

        srcCompCost = (double(clock() - begin) - hIO.getIOCPUTime()) / CLOCKS_PER_SEC; // source cpu time

        //std::cout << elapsedTime1 << "\t" << elapsedTime  << endl;
        //std::cout << hIO.getCommCostDistribute() << endl;
        //std::cout << srcCompCost  << endl;
        //std::cout << "master ends..." << endl;

        // report results

        return globalCnt;
    }else // Worker part
    {

        //std::cout << "worker begins..." << endl;
        //cout << "Id:" << hIO.getWorkerId() << endl;  

        Worker  worker(memSize, seed + hIO.getWorkerId());
        Edge edge;
        while(hIO.recvEdge(edge))
        {

            worker.processEdge(edge);

        }
    	worker.finalCnt();

        //std::cout << "Worker: "  << double(clock() - begin) / CLOCKS_PER_SEC << "\t" << hIO.getIOCPUTime() / CLOCKS_PER_SEC << "\t" <<  workerCompCost << endl;
        //std::cout << "worker ends" << endl;

        // send counts to master
        hIO.sendCnt(worker.getGlobalCnt(), worker.getLocalCnt());

        double workerCompCost = (double(clock() - begin) - hIO.getIOCPUTime()) / CLOCKS_PER_SEC; // source cpu time

        hIO.sendTime(workerCompCost);
        return 0;
    }

}

void run_exp (const char* input, const char* outPath, MPIIO &hIO, int workerNum, int memSize, int repeat, int bufLen, double tolerance)
{

    int seed = 0;

    struct timeval diff, startTV, endTV;

	if (hIO.isMaster())
	{
		struct stat sb;
		if (stat(outPath, &sb) == 0)
		{
			if (S_ISDIR(sb.st_mode)) //TODO. directory is exists
				;
			else if (S_ISREG(sb.st_mode)) //TODO. No directory but a regular file with same name
				;
			else // TODO. handle undefined cases.
				;
		} 
		else 
		{
			mkdir(outPath, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
		}
	}

    for(int i =0 ; i < repeat; i++) {

        if (hIO.isMaster()) {

            gettimeofday(&startTV, NULL);//开始时间 

            std::vector<float> nodeToCnt; //Local Count 

            double srcCompCost = 0;
            double workerCompCostMax = 0;
            double workerCompCostSum = 0;
            
			double globalCnt = run_mpi(input, hIO, workerNum, memSize, bufLen, tolerance, seed + repeat * workerNum * i, nodeToCnt, srcCompCost, workerCompCostMax, workerCompCostSum);

            gettimeofday(&endTV, NULL);

            timersub(&endTV, &startTV, &diff); //计算时间差 diff 

            double elapsedTime = diff.tv_sec * 1000 + diff.tv_usec / 1000 ; //单位：ms 
            
            
			cout << i << ":\t" << elapsedTime << "\tms" << endl;
			cout << i << ":\t" << hIO.getCommCostDistribute() << "\t" << endl;
			print_cnt(outPath, globalCnt, nodeToCnt, i, elapsedTime);

        } else {

            double srcCompCost = 0;
            double workerCompCostMax = 0;
            double workerCompCostSum = 0;
            std::vector<float> nodeToCnt;
            run_mpi(input, hIO, workerNum, memSize, bufLen, tolerance, seed + repeat * workerNum * i, nodeToCnt, srcCompCost, workerCompCostMax, workerCompCostSum);
        }
    }
}

void print_cnt(const char* outPath, double globalCnt, const std::vector<float> &localCnt, int id, double elapsedTime)
{

	// Print global count
	std::ostringstream gCntFileName;
	gCntFileName << outPath << "/global" << id << ".txt";
	std::fstream	gfp;
	gfp.open(gCntFileName.str(), std::fstream::out | std::fstream::trunc);
	gfp << std::setprecision(std::numeric_limits<double>::max_digits10) <<  globalCnt << endl;
	gfp.close();
	
	std::ostringstream timeFileName;
	timeFileName << outPath << "/time" << id << ".txt";
	std::fstream	time;
	time.open(timeFileName.str(), std::fstream::out | std::fstream::trunc);
	time << std::setprecision(std::numeric_limits<double>::max_digits10) <<  elapsedTime << endl;
	time.close();

	std::ostringstream lCntFileName;
	lCntFileName << outPath << "/local" << id << ".txt";
	std::fstream	lfp;
	lfp.open(lCntFileName.str(), std::fstream::out | std::fstream::trunc);

	for (int nid = 0; nid < localCnt.size(); nid++)
	{
		lfp << nid << "\t"  << localCnt[nid] << endl;
	}
	lfp.close();
}
