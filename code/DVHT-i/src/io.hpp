#ifndef _IO_HPP_
#define _IO_HPP_

#include "base_struct.hpp"
#include <mpi.h>
#include <cstdlib>
#include <vector>
#include <unordered_map>
#include <iostream>
#include <stddef.h>
#include <ctime>
#include <cmath>
#include <sys/time.h>

class MPIIO {
_PRIVATE:
/*
 *		Sub-class and static member and methods
 */
	class EdgeContainer {//初始化的时候，buf[0]指向一个含1000条Edge的数组，buf[1]指向另一个含1000条边的数组
	_PRIVATE:
		MID							mid;
		unsigned int				bit;
		unsigned int				qit;
		bool						isEmpty;
		double						ioCPUTime; // cpu time used for MPI communication

	public:
		Edge						*buf[2];// buf 是一个指针数组，大小为2，存了两个指向边的指针，buf[0]中指向一条边，buf[1]中指向另一条
		MPI_Request 				req[2];

		EdgeContainer(): bit(0), qit(0), isEmpty(true), ioCPUTime(0),
				buf{nullptr, nullptr}, req{MPI_REQUEST_NULL, MPI_REQUEST_NULL}{}

		~EdgeContainer()
		{
			if (buf[0] != nullptr)
			{
				delete[] buf[0];
				buf[0] = nullptr;
			}

			if (buf[1] != nullptr)
			{
				delete[] buf[1];
				buf[1] = nullptr;
			}

			waitIOCompletion(req[0]);
			waitIOCompletion(req[1]);
		}

		inline bool init(MID iMid)
		{
			mid = iMid;
			buf[0] = new Edge[lenBuf]; // buf[0] 指向一个含1000条Edge的数组，则 buf[0][1]表示该数组中第二条边
			buf[1] = new Edge[lenBuf];
			return true;
		}

		// Methods for receiver
		inline void getNext(Edge &oEdge)
		{//传进来一个地址，这个方法是 worker 在调用, 取出自身 ebuf[0]中的一条边
			if (isEmpty)
			{
				clock_t begin = clock();

				MPIIO::IrecvEdge(buf[qit], req[qit]);
				waitIOCompletion(req[qit]);
				MPIIO::IrecvEdge(buf[(qit+1)%2], req[(qit+1)%2]);

				ioCPUTime += double(clock() - begin);

				isEmpty = false;
			}
			else if (bit == lenBuf)
			{
				clock_t begin = clock();

				bit = 0;
				MPIIO::IrecvEdge(buf[qit], req[qit]);
				qit = (qit + 1) % 2;
				waitIOCompletion(req[qit]);

				ioCPUTime += double(clock() - begin);
			}

			oEdge = buf[qit][bit++];

			return;
		}

		void cleanup(){
			int flag(0);
			MPI_Status st;
			for (int i = 0; i < 2; i++)
			{
				if (req[i] != MPI_REQUEST_NULL)
				{
					MPI_Test(&req[i], &flag, &st);
					if (flag == false)
					{
						MPI_Cancel(&req[i]);
					}
					req[i] = MPI_REQUEST_NULL;
				}
			}
		}

		// Methods for sender
		// putNext完成的是：通过 IsendEdge调用 MPI_Isend 方法，实现将 buf[qit]中的边发送给对应目标worker
		inline bool putNext(const Edge &iEdge)
		{
		    /////////////////////////////cout << "putNext............ iEdge = " << iEdge <<"     bit = " << bit << "     lenBuf = " << lenBuf << endl;
		    //cout << "bit =  "<<bit << "    " << lenBuf <<endl;
			buf[qit][bit++] = iEdge;

			if (bit == lenBuf)
			{
			    //cout << "putNext........;;;;;;;;. iEdge = " << iEdge << endl;
				clock_t begin = clock();
				bit = 0;
				MPIIO::IsendEdge(buf[qit], mid, req[qit]);

				qit = (qit + 1) % 2;
				waitIOCompletion(req[qit]);

				ioCPUTime += double(clock() - begin);
			}

			return true;
		}

		void flushSend()
		{

			clock_t begin = clock();

			if (bit != 0)
			{
				MPIIO::IsendEdge(buf[qit], mid, req[qit]);
			}

			for (int i = 0; i < 2; i++)
			{
				waitIOCompletion(req[i]);
			}

			ioCPUTime += double(clock() - begin);
		}
	};

	static const int 		TAG_STREAM = 0;
	static const int 		TAG_RET	= 1;
	static MPI_Datatype 	MPI_TYPE_EDGE;
	static MPI_Datatype 	MPI_TYPE_ELEMCNT;
	static const Edge 		END_STREAM;
	static unsigned short	lenBuf;

	inline static void waitIOCompletion(MPI_Request &iReq)
	{
		MPI_Status 	st;
		(MPI_REQUEST_NULL != iReq) && MPI_Wait(&iReq, &st);
		iReq = MPI_REQUEST_NULL;
		return;
	}

	static bool IrecvEdge(Edge *buf, MPI_Request &iReq);
	static bool IsendEdge(Edge *buf, int dst, MPI_Request &iReq);


/*
 *		Sub-class and static member and methods
 */

	MPI_Request				req;
	int 					rank;
	int 					szProc;
	int 					workerNum; // number of actual workers
	int                    aggreNum;
	int                     masterNum;
	//int **              ranks;
	int                     colnum;//每一组中的进程数
	int                     aggreGroupWorkerNum;// Aggregator组中的数量 =  aggreNum + 1（包括master）
	int *                root;//每组的root.  root[0] ....root[aggreNum - 1]
    int **             newranks;// 用于分组的进程号数组, newranks[aggreNum][colnum]
    int **              ori;//
    int *               tmprank;
    MPI_Group *          group;//分组, group[0]...group[aggreNum-1],,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
    MPI_Comm *          comm;//通信域, comm[0]...comm[aggreNum-1] ,,,,,,,,,,,,,,,,,,,,,,,同group，再加一个

    MPI_Group        aggreGroup;
    MPI_Comm        aggreComm;// aggregator组的通信域
    int*                 aggreRank;// aggre组中的进程号数组[ workerNum, ......, szProc - 1, 0]
    int                    aggreRoot;//

	vector<EdgeContainer>	eBuf;
	long						commCostDistribute;
    long						commCostGather;
	long						ioCPUTime;


public:

	MPIIO(int &argc, char** &argv, int aggNum);
	~MPIIO(){}

	// initialize
	void init(int buffersize, int workerNum);
	void cleanup(int aggreNum);

	// whether this thread is a master or not
	bool isMaster();

	// whether this thread is a valid worker or not
	// bool isActiveWorker();

	// worker Id
	MID getWorkerId();

	//@YuMD
	int getAgrregatorNum();

	int getWorkerNum();

	// communication cost (logical)
	long getCommCostDistribute();
	long getCommCostGather();

	// Edge
	bool sendEdge(const Edge &iEdge, MID dst);
	bool bCastEdge(Edge& iEdge, MID dst1, MID dst2);
	bool recvEdge(Edge &oEdge);
	//bool sendEdge2Worker(Edge &iEdge, MID dst) ;

	// Receiving data
	// @bamboo
	// worker 给对应的 aggregator 发，需要mid
	bool sendCnt(double gCnt, unordered_map<VID, float> &lCnt, MID glo_aggreid, MID loc_aggreid_src, MID loc_aggid_dst);
	//bool sendCnt_v3(double gCnt, unordered_map<VID, float> &lCnt, MID glo_aggreid, int worker, int aggNum);
	bool sendCnt_v3_global(double gCnt, int commid, int worker, int aggNum);

	bool sendCnt2Master_global(double gCnt, MID arank);
	bool recvCnt_v3_global(double &gCnt, int commid);
	bool recvCnt_Maser_global(double &gCnt);

    bool recvCnt(VID maxVId, double &gCnt, std::vector<float> &lCnt);

	// CPU Time used for MPI communication
	double getIOCPUTime();

	// send computational time info
	bool sendTime(double compTime);
	bool recvTime(double &compTimeMax, double &compTimeSum);

	// Control flow
	bool sendEndSignal();

	// Get variable
	int getRank(){ return rank;}
	int getSzProc(){ return szProc;}

};

#endif // _IO_HPP_
