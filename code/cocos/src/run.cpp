#include "run.hpp"

double run_mpi(const char* filename, MPIIO &hIO, int workerNum, double theta, int memSize, int lenBuf, double tolerance, unsigned int seed, std::vector<float> & oLocalCnt, double &srcCompCost, double &workerCompCostMax, double &workerCompCostSum)
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
                int hashSrc = edge.src % workerNum;
				int hashDst = edge.dst % workerNum;
                edge.hashSrc = hashSrc;
				edge.hashDst = hashDst;
                // cout << "u:" << hashSrc << " v:" << hashDst << endl;

    ///////////////////////////////////////////////////////////////
                // int hashSrc = edge.src % 10;
				// int hashDst = edge.dst % 10;
                // if(hashSrc <= 5){
                //     edge.hashSrc = 0;
                // }else{
                //     edge.hashSrc = 1;
                // }

                // if(hashDst <= 5){
                //     edge.hashDst = 0;
                // }else{
                //     edge.hashDst = 1;
                // }
                // cout << "u:" << edge.hashDst << " v:" << edge.hashDst << endl;
    ///////////////////////////////////////////////////////////////////		
				if (hashSrc == hashDst){
					hIO.sendEdge(edge, hashSrc);
				}else{
					hIO.bCastEdge(edge, dst1, dst2);
				}
            }
        }

        hIO.sendEndSignal();
        
        double masterTimeCost = double(clock() - begin - hIO.getIOCPUTime()) / CLOCKS_PER_SEC * 1000; 

        std::cout << "Master: " << masterTimeCost << endl;

        // Gather results from curWorkers
        double globalCnt = 0;

        clock_t begin1 = clock();

        // communication cost for gather
        hIO.recvCnt(source.getMaxVId(), globalCnt, oLocalCnt);

        //std::cout << source.getMaxVId() << "\t" << globalCnt << "\t" << oLocalCnt.size();

        hIO.recvTime(workerCompCostMax, workerCompCostSum);

        std::cout << "WorkerMax:" << workerCompCostMax << "\t WorkerSum:" << workerCompCostSum << std::endl;

        double aggTimeCost = (double(clock() - begin1)) / CLOCKS_PER_SEC * 1000; // source cpu time
        std::cout << "Aggegator: " << aggTimeCost << std::endl;
        //std::cout << elapsedTime1 << "\t" << elapsedTime  << endl;
        //std::cout << hIO.getCommCostDistribute() << endl;
        //std::cout << srcCompCost  << endl;
        //std::cout << "master ends..." << endl;

        // report results
		cout <<"globalCnt:" << globalCnt << endl;
        return globalCnt;
    }else // Worker part
    {

        //std::cout << "worker begins..." << endl;
        int id = hIO.getWorkerId();

        //cout << "Memsize:" << memSize << endl;

        Worker  worker(memSize, seed + hIO.getWorkerId());
        Edge edge;
        while(hIO.recvEdge(edge))
        {
            worker.processEdge(edge, workerNum, id);
        }

        // std::cout << "Worker: "  << double(clock() - begin) / CLOCKS_PER_SEC * 1000 << "\t" << hIO.getIOCPUTime() / CLOCKS_PER_SEC << "\t" <<  workerCompCost << endl;
        // std::cout << "Worker: "  << double(clock() - begin) / CLOCKS_PER_SEC * 1000 << "\t" << hIO.getIOCPUTime() / CLOCKS_PER_SEC * 1000 << "\t" << endl;
        //std::cout << "worker ends" << endl;

        // send counts to master
        hIO.sendCnt(worker.getGlobalCnt(), worker.getLocalCnt());
        //cout << "globalCnt:" << worker.getGlobalCnt() << endl;

        double workerTimeCost = (double(clock() - begin) - hIO.getIOCPUTime()) / CLOCKS_PER_SEC * 1000; // source cpu time
        //std::cout << "Worker: "  << workerTimeCost << "\t" << hIO.getIOCPUTime() / CLOCKS_PER_SEC * 1000 << std::endl ;
        hIO.sendTime(workerTimeCost);
        return 0;
    }

}

void run_exp (const char* input, const char* outPath, MPIIO &hIO, int workerNum, double theta, int memSize, int repeat, int bufLen, double tolerance)
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

            gettimeofday(&startTV, NULL);//��ʼʱ�� 

            std::vector<float> nodeToCnt; //Local Count 

            double srcCompCost = 0;
            double workerCompCostMax = 0;
            double workerCompCostSum = 0;
            
			double globalCnt = run_mpi(input, hIO, workerNum, theta, memSize, bufLen, tolerance, seed + repeat * workerNum * i, nodeToCnt, srcCompCost, workerCompCostMax, workerCompCostSum);

            gettimeofday(&endTV, NULL);

            timersub(&endTV, &startTV, &diff); //����ʱ��� diff 

            double elapsedTime = diff.tv_sec * 1000 + diff.tv_usec / 1000 ; //��λ��ms 
            
            
			cout << i << ":\t" << elapsedTime << "\tms" << endl;
			cout << i << ":\t" << hIO.getCommCostDistribute() << "\t" << endl;
			print_cnt(outPath, globalCnt, nodeToCnt, i, elapsedTime);

        } else {

            double srcCompCost = 0;
            double workerCompCostMax = 0;
            double workerCompCostSum = 0;
            std::vector<float> nodeToCnt;
            run_mpi(input, hIO, workerNum, theta, memSize, bufLen, tolerance, seed + repeat * workerNum * i, nodeToCnt, srcCompCost, workerCompCostMax, workerCompCostSum);
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
