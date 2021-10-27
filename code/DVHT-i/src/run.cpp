#include "run.hpp"

double run_mpi(const char* filename, MPIIO &hIO, int workerNum, int aggNum, int memSize, int lenBuf, double tolerance, unsigned int seed, std::vector<float> & oLocalCnt, double &srcCompCost, double &workerCompCostMax, double &workerCompCostSum) {
    //lenBuf = 1000, tolerance = 0.2
//lenBuf 此处相当于一个窗口缓冲区的量，每当 master 接收到1000条流后，就一次性发送这1000条。

    //Computational Cost
    clock_t begin = clock();

    hIO.init(lenBuf, workerNum);


    // Sourcce init


     if(hIO.isMaster()){
        Source source;
        Edge edge;//在base_struct.cpp 中定义，两个参数 src dst
        MID dst1(0);//short dst1 = 0; 分发给 0 号机器。
        MID dst2(0);
        EdgeParser parser(filename);// 在base_struct.cpp 中定义， 有一个函数，getEdge

        while (parser.getEdge(edge)) { // Stream edges
            // @YuMD 获得两个顶点的哈希值
            dst1 = edge.src % workerNum;
            dst2 = edge.dst % workerNum;
            //////////////////////////////////////////////cout << "run_exp......    master中准备发的边， edge.src = " << edge.src <<"      edge.dst =  " <<edge.dst << "    dst1 = " << dst1 << "         dst2 = " << dst2<<endl;

            if(edge.src != edge.dst) {
                source.processEdge(edge, dst1, dst2);//没有任何改变，只是 source.maxVId 或许有改变，对 edge、dst1、dst2 都没有任何改变
                //@YuMD 广播
                    hIO.bCastEdge(edge, dst1, dst2);
                }
        }
        hIO.sendEndSignal();



        //Master is end.
     } else if(hIO.getWorkerId() < workerNum) { // Worker part
        std::cout << "worker "<< hIO.rank <<" begins..." << endl;
        Worker  worker(memSize, seed + hIO.getWorkerId());
        Edge edge;
        MID id = hIO.getWorkerId();
        bool flag = false;
        // @bamboo
        // 每个 worker 处理自己上的边, 修改了processEdge函数，增加了 flag 参数代表是否处理
        //1. 对 u v 哈希，判断是否要处理并调用processEdge对边进行处理
        //2. 对(workerid+1)做处理，判断其是分配到了哪一个通信域
        //..................//2. 对 workerid 哈希，判断本机的globalCnt发送到哪一个 aggregator 上进行聚合
        //.................. //3. 对 u 哈希，判断该顶点的 localCnt 发送到哪个 aggregator
        //..................//4. 对 v 哈希，判断该顶点的 localCnt 发送到哪个 aggregator
        //..................//5. 调用 sendCnt（）实现与 aggregator 的通信

        int edgeCount = 0;
        while(hIO.recvEdge(edge)) {
//                //////////////////////////////////////////////cout << "run_mpi.......  edge.src = " << edge.src << "      edge.dst = " << edge.dst << endl;
//
//            if(edge.src % workerNum == id || edge.dst % workerNum == id)
//                flag = true;
//            else
//                flag = false;
//
//            //////////worker.processEdge(edge, flag, id,);///////44444444444444444444
            worker.processEdge1(edge, id, workerNum);
            edgeCount++;
        }
        cout << hIO.rank << "-edgeNum:" << edgeCount << endl;
        std::cout << worker.getGlobalCnt() << std::endl;
        cout << hIO.rank << "-globalCnt1:" << worker.getGlobalCnt() << endl;
        //std::cout << "Worker: "  << double(clock() - begin) / CLOCKS_PER_SEC << "\t" << hIO.getIOCPUTime() / CLOCKS_PER_SEC << "\t" <<  workerCompCost << endl;
        //std::cout << "worker ends" << endl;


        //@YuMD
        // rank = id+1 的worker 分配给 comm[commid] 通信域
        //..................// 对workerid求哈希值，找到其对应的聚合器。
        //..................// 有 aggNum 个聚合器，其 mid = getWorkerid - workerNum
        //..................// 做计算时用 - workerNum后的id， 发送时要用真正的 rank 值。
        //////////////////////////////////////////////cout<<"endssss....................."<<endl;
        int  colnum = (int)ceil(1.0*workerNum / aggNum );
        int commid = (int)ceil((1.0 *(id+1)) / colnum) - 1; // 通信域的号
         //////////////////////////////////////////////cout<< "This is worker............. workerid = "<< hIO.getWorkerId() <<"    workerrank = " << hIO.getRank()<< "    workercommid = "  << commid<< endl;


        // send counts to aggregator
        // 每个worker出来的 nodeToCnt 都不一样，而 nodeToCnt 中的每一个顶点都要发往不同的 aggregator
        // 目前方法： 将 workerNum 和 aggNum 发给 sendCnt_v3， 在其中做计算和遍历分发

        hIO.sendCnt_v3_global(worker.getGlobalCnt(), commid, workerNum, aggNum);
        //hIO.sendCnt_v3(worker.getGlobalCnt(), worker.getLocalCnt(), glo_aggid + workerNum + 1,workerNum, aggNum);
        //hIO.sendCnt(worker.getGlobalCnt(), worker.getLocalCnt(), glo_aggid + workerNum + 1, loc_aggid + workerNum + 1);
        

        double workerCompCost = (double(clock() - begin) - hIO.getIOCPUTime()) / CLOCKS_PER_SEC; // source cpu time

        //hIO.sendTime(workerCompCost);
        return 0;
    }else{//aggregator...............................
        // 每个aggre判断自己在哪个通信域中, 在comm[aid]中

        int aid = hIO.getWorkerId() - workerNum ; // 从0开始

        //////////////////////////////////////////////std::cout << "Aggregator........... 全局rank : " << hIO.getRank()<< "\t" <<"getWorkerId : " << hIO.getWorkerId()  << "\t" <<"commid: "<< aid<<endl;

        // Gather results from curWorkers
        double globalCnt = 0;

        //..............................// communication cost for gather
        //..............................// source.getMaxId()返回的是目前所有顶点中最大的Id


        //hIO.recvCnt(source.getMaxVId(), globalCnt, oLocalCnt);
        hIO.recvCnt_v3_global(globalCnt, aid);


        //std::cout << "source.getMaxVId() : " << source.getMaxVId() << "\t" <<"globalCnt : " << globalCnt << "\t" <<"oLocalCnt.size() : "<< oLocalCnt.size();

        //hIO.recvTime(workerCompCostMax, workerCompCostSum);

        srcCompCost = (double(clock() - begin) - hIO.getIOCPUTime()) / CLOCKS_PER_SEC; // source cpu time

	//std::cout << "Aggregator: " << double(clock() - begin) / CLOCKS_PER_SEC<<endl;
        //std::cout << elapsedTime1 << "\t" << elapsedTime  << endl;
        //std::cout << hIO.getCommCostDistribute() << endl;
        //std::cout << srcCompCost  << endl;
        //std::cout << "master ends..." << endl;

        // report resul	ts

        return globalCnt;
    }

}

void run_exp (const char* input, const char* outPath, MPIIO &hIO, int workerNum, int aggNum, int memSize, int repeat, int bufLen, double tolerance) {

    int seed = 0;

    struct timeval diff, startTV, endTV;
//@YuMD
    clock_t start = clock();
    //cout << "run_exp-----start: "<< start << endl;

    if (hIO.isMaster()) {
        struct stat sb;
        if (stat(outPath, &sb) == 0) {
            if (S_ISDIR(sb.st_mode)) //TODO. directory is exists
                ;
            else if (S_ISREG(sb.st_mode)) //TODO. No directory but a regular file with same name
                ;
            else // TODO. handle undefined cases.
                ;
        } else {
            mkdir(outPath, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
        }
    }

    //repeat = trial
    for(int i = 0 ; i < repeat; i++) {


        if (hIO.isMaster()) {

            gettimeofday(&startTV, NULL);

            std::vector<float> nodeToCnt;

            double srcCompCost = 0;
            double workerCompCostMax = 0;
            double workerCompCostSum = 0;
            run_mpi(input, hIO, workerNum, aggNum, memSize, bufLen, tolerance, seed + repeat * workerNum * i, nodeToCnt, srcCompCost, workerCompCostMax, workerCompCostSum);

            double totalCnt = 0;
	        hIO.recvCnt_Maser_global(totalCnt);
			cout.setf(ios_base::fixed,ios_base::floatfield);
	        
	        
	        gettimeofday(&endTV, NULL);
	        timersub(&endTV, &startTV, &diff); //����ʱ��� diff 
	        double elapsedTime = diff.tv_sec * 1000 + diff.tv_usec / 1000 ; //��λ��ms
			cout <<i<<":\t" << "globalCnt:" << totalCnt << endl;
			cout <<i<<":\t" << "elapsedTime:" << elapsedTime << "\tms" <<endl;
			print_cnt(outPath, totalCnt, i,elapsedTime);
            //gettimeofday(&endTV, NULL);

        //std::cout << "Master: " << double(clock() - begin) / CLOCKS_PER_SEC << "\t" << hIO.getIOCPUTime() / CLOCKS_PER_SEC << "\t" <<  srcCompCost << endl;
        }else if(hIO.getWorkerId() < (MID)workerNum){ // 是worker........................................
            double srcCompCost = 0;

            double workerCompCostMax = 0;
            double workerCompCostSum = 0;
            std::vector<float> nodeToCnt;
            run_mpi(input, hIO, workerNum, aggNum, memSize, bufLen, tolerance, seed + repeat * workerNum * i, nodeToCnt, srcCompCost, workerCompCostMax, workerCompCostSum);
        }else{ // 是 aggregator................................................
            std::vector<float> nodeToCnt;

            double srcCompCost = 0;
            double workerCompCostMax = 0;
            double workerCompCostSum = 0;

            double totalCnt = run_mpi(input, hIO, workerNum, aggNum, memSize, bufLen, tolerance, seed + repeat * workerNum * i, nodeToCnt, srcCompCost, workerCompCostMax, workerCompCostSum);

            //给master发globalCount
            hIO.sendCnt2Master_global(totalCnt, hIO.getWorkerId() +1);//所有aggregator上的这个totalCnt的值

            gettimeofday(&endTV, NULL);

            timersub(&endTV, &startTV, &diff);

            double elapsedTime = diff.tv_sec * 1000 + diff.tv_usec / 1000 ;
            //cout << "```````````````````````````globalCnt`````````````````````````````"<<endl;
            //cout<<globalCnt<<endl;
            //cout<<"`````````````````````````````"<<hIO.getWorkerId() <<"```````````````````````"<<endl;

            //print_cnt(outPath, globalCnt, nodeToCnt, i);

        }


    }


    clock_t eeeend = clock();
    //////////////////////////////////////////////cout << "run_exp-----end: "<< eeeend << "\t" << double(eeeend - start) / CLOCKS_PER_SEC << endl;

}

void print_cnt(const char* outPath, double globalCnt, int id, double elapsedTime)
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
}

