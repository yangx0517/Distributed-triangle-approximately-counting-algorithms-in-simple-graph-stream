#include "io.hpp"

MPI_Datatype 		MPIIO::MPI_TYPE_EDGE;
MPI_Datatype 		MPIIO::MPI_TYPE_ELEMCNT;
const Edge 			MPIIO::END_STREAM(INVALID_VID, INVALID_VID);

unsigned short		MPIIO::lenBuf;

MPIIO::MPIIO(int &argc, char** &argv)//, bit(0), lenCQ(1), cqit(0)
{
	// Establish connection
	MPI_Init(&argc, &argv);

	// Get connection information
	MPI_Comm_size(MPI_COMM_WORLD, &szProc);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	// Initialize and Register struct EDGE, ELEMCNT information
	int          lenAttr[Edge::szAttr] = {1, 1};
	MPI_Datatype arrType[Edge::szAttr] = {MPI_UNSIGNED, MPI_UNSIGNED}; //�������� 

	MPI_Aint     offsets[Edge::szAttr]; //��ַƫ������ 
	offsets[0] = offsetof(Edge, src);
	offsets[1] = offsetof(Edge, dst);
	MPI_Type_create_struct(Edge::szAttr, lenAttr, offsets, arrType, &MPI_TYPE_EDGE);   //MPI�������� 
	MPI_Type_commit(&MPI_TYPE_EDGE); //�ύ������ 

	//�ֲ������θ���ElemCnt 

	arrType[0] = MPI_UNSIGNED;
	arrType[1] = MPI_DOUBLE;
	offsets[0] = offsetof(ElemCnt, vid);
	offsets[1] = offsetof(ElemCnt, cnt);
	MPI_Type_create_struct(ElemCnt::szAttr, lenAttr, offsets, arrType, &MPI_TYPE_ELEMCNT);
	MPI_Type_commit(&MPI_TYPE_ELEMCNT);
}

// Initialize requests and buffers
void MPIIO::init(int lenBuf, int workerNum)
{
	commCostDistribute = 0;
    commCostGather = 0;
    ioCPUTime = 0;

    eBuf.clear();
    MPIIO::lenBuf = lenBuf;
    MPIIO::workerNum = workerNum;

    if (rank == MPI_MASTER)
    {
        eBuf.resize(workerNum);
        for (int i = 0; i < workerNum; i++)
        {
            eBuf[i].init(i);
        }
    }
    else
    {
        eBuf.resize(1);
        eBuf[0].init(getWorkerId());
    }
}


bool MPIIO::isMaster()
{
    return rank == MPI_MASTER;
}

//bool MPIIO::isActiveWorker()
//{
//    return rank <= workerNum;
//}

MID MPIIO::getWorkerId()
{
    return (MID)(rank - 1);
}

long MPIIO::getCommCostDistribute()
{
    return commCostDistribute;
}

long MPIIO::getCommCostGather()
{
    return commCostGather;

}

void MPIIO::cleanup()
{
	MPI_Finalize();
}

bool MPIIO::bCastEdge(Edge &iEdge, MID dst1, MID dst2)
{
	Edge tmpEdge(iEdge);
	for (int mit = 0; mit < workerNum; mit++)
	{
		eBuf[mit].putNext(tmpEdge);
	}
	commCostDistribute += workerNum;

	return true;
}

bool MPIIO::IrecvEdge(Edge *buf, MPI_Request &iReq)
{
	return (MPI_SUCCESS == MPI_Irecv(buf, lenBuf, MPI_TYPE_EDGE, MPI_MASTER, TAG_STREAM, MPI_COMM_WORLD, &iReq)); //���������� 
	//waitIOCompletion(iReq);
}

bool MPIIO::IsendEdge(Edge *buf, int mid, MPI_Request &iReq){
	MPI_Isend(buf, lenBuf, MPI_TYPE_EDGE, mid + 1, TAG_STREAM, MPI_COMM_WORLD, &iReq);
	return true;
}

bool MPIIO::recvEdge(Edge &oEdge)
{
	eBuf[0].getNext(oEdge);
	if (oEdge == END_STREAM)
	{
		eBuf[0].cleanup();
	}
	return (oEdge != END_STREAM);
}

bool MPIIO::sendEndSignal()
{
	Edge signal(END_STREAM);
	for (int mit = 0; mit < workerNum; mit++)
	{
		eBuf[mit].putNext(signal);
		eBuf[mit].flushSend();
	}
	return true;
}

// 
bool MPIIO::sendCnt(double gCnt, unordered_map<VID, float> &lCnt)
{
    clock_t begin = clock();
	MPI_Reduce(&gCnt, nullptr, 1, MPI_DOUBLE, MPI_SUM, MPI_MASTER, MPI_COMM_WORLD);
    VID maxVId;
    MPI_Bcast(&maxVId, 1, MPI_UNSIGNED, MPI_MASTER, MPI_COMM_WORLD);
    ioCPUTime += double(clock() - begin);


    float* lCntArr = new float[maxVId+1];
    std::fill_n(lCntArr, maxVId+1, 0.0);
    unordered_map<VID, float>::const_iterator it;
    for (it = lCnt.begin(); it != lCnt.end(); it++) {
        lCntArr[it->first] = it->second;
    }

    begin = clock();
    MPI_Reduce(lCntArr, nullptr, maxVId+1, MPI_FLOAT, MPI_SUM, MPI_MASTER, MPI_COMM_WORLD);
    ioCPUTime += double(clock() - begin);

    delete lCntArr;
    return true;

  
}

bool MPIIO::recvCnt(VID maxVId, double &gCnt, std::vector<float> &lCnt)
{
	

	double empty = 0;
    clock_t begin = clock();
	MPI_Reduce(&empty, &gCnt, 1, MPI_DOUBLE, MPI_SUM, MPI_MASTER, MPI_COMM_WORLD);
    MPI_Bcast(&maxVId, 1, MPI_UNSIGNED, MPI_MASTER, MPI_COMM_WORLD);
    ioCPUTime += double(clock() - begin);

    float* lCntArr = new float[maxVId+1];
    std::fill_n(lCntArr, maxVId+1, 0.0);

    begin = clock();
    MPI_Reduce(MPI_IN_PLACE, lCntArr, maxVId+1, MPI_FLOAT, MPI_SUM, MPI_MASTER, MPI_COMM_WORLD);
    ioCPUTime += double(clock() - begin);

    commCostGather = (maxVId + 1) * (getSzProc()-1);

    lCnt.insert(lCnt.end(), &lCntArr[0], &lCntArr[maxVId]);
    delete lCntArr;


	return true;
}

double MPIIO::getIOCPUTime()
{
	if (rank == MPI_MASTER)
	{
		double totalIOCPUTime = ioCPUTime;
		for (int i = 0; i < workerNum; i++)
		{
			totalIOCPUTime += eBuf[i].ioCPUTime;
		}
		return totalIOCPUTime;
	}
	else
	{
		return eBuf[0].ioCPUTime;
	}
}

bool MPIIO::sendTime(double compTime)
{
    MPI_Reduce(&compTime, nullptr, 1, MPI_DOUBLE, MPI_MAX, MPI_MASTER, MPI_COMM_WORLD);
    MPI_Reduce(&compTime, nullptr, 1, MPI_DOUBLE, MPI_SUM, MPI_MASTER, MPI_COMM_WORLD);
    return true;
}

bool MPIIO::recvTime(double &compTimeMax, double &compTimeSum)
{
	double empty = 0;
	MPI_Reduce(&empty, &compTimeMax, 1, MPI_DOUBLE, MPI_MAX, MPI_MASTER, MPI_COMM_WORLD);
    MPI_Reduce(&empty, &compTimeSum, 1, MPI_DOUBLE, MPI_SUM, MPI_MASTER, MPI_COMM_WORLD);
	return true;

}
