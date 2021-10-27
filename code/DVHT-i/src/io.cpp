#include "io.hpp"

MPI_Datatype 		MPIIO::MPI_TYPE_EDGE;
MPI_Datatype 		MPIIO::MPI_TYPE_ELEMCNT;
const Edge 			MPIIO::END_STREAM(INVALID_VID, INVALID_VID);

unsigned short		MPIIO::lenBuf;

MPIIO::MPIIO(int &argc, char** &argv, int aggreNum) { //, bit(0), lenCQ(1), cqit(0)
    // Establish connection

    MPI_Init(&argc, &argv);

    // 获得参与并行的核的总数，存在 szProc变量 中
    MPI_Comm_size(MPI_COMM_WORLD, &szProc);
    // 获得自己所在进程的序列号，存在 rank 变量中
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    masterNum = 1;///////////////////////////////4444444444444444444444444

    //@YuMD
    // 分组， 获得进程组 world_group，即 MPI_COMM_WORLD
    MPI_Group world_group;
    MPI_Comm_group(MPI_COMM_WORLD, &world_group);  //MPI_Comm_group创建的通信器，与其他通信器对象的工作方式相同，只是不能进行进程间通信

    workerNum = szProc - masterNum - aggreNum;////////////////////////444444444444444444

    root = (int *)malloc(sizeof(int) * aggreNum);

    aggreGroupWorkerNum = aggreNum + 1;//所有的 aggregator 和 主MASTER

    newranks = (int **)malloc(sizeof(int *) * (aggreNum - 1));
    aggreRank = (int*)malloc(sizeof(int) * (aggreGroupWorkerNum));

    colnum = (int)ceil(1.0*workerNum / (aggreNum) ); // =3
    std::cout<< "workerNum:" << workerNum << "\t" << "aggregaterNum:" << aggreNum << "\t" << "colnum:" << colnum << endl;

    ori = (int **)malloc(sizeof(int*) * aggreNum);//

    group = (MPI_Group *)malloc(sizeof(MPI_Group) * (aggreNum));


    comm = (MPI_Comm *)malloc(sizeof(MPI_Comm) * (aggreNum));
    tmprank =  (int *)malloc(sizeof(int) * (workerNum - (aggreNum-1)*colnum));

    int t;
    for(t = 0; t < workerNum - (aggreNum-1)*colnum; t++){
        *(tmprank + t) = t + 1 + (aggreNum-1)*colnum;
        //printf("%d\n",*(tmprank + t));
    }

    // for(t = 0; t < workerNum % colnum; t++){
    //     *(tmprank + t) = t + 1 + (aggreNum-1)*colnum;
    //     //printf("%d\n",*(tmprank + t));
    // }

    *(tmprank + t) = szProc - 1;

    // 为进程组分配空间，每个进程组中有 colnum + 1 个进程号，包括colnum 个worker 和1 个aggregator
    for(int k = 0; k <  aggreNum - 1; k ++)
        newranks[k] = (int *)malloc(sizeof(int) * (colnum+1));
    // 分进程组  newranks[0] ～ newranks[aggreNum-2] 是分出来的 aggreNum-1 个进程组, 最后一个存在了tmprank中
    int ii, j;
    for( ii = 0; ii < aggreNum - 1; ii++){
        for( j = 0; j < colnum; j++){
                newranks[ii][j] = ii * colnum + j +1 ;//存的是rank号，worker是从1 开始的
        }
        newranks[ii][j] = workerNum + ii +1;///////////////加入aggregator
    }//分成了 aggreNum -1 个组，newranks[0] ... newranks[aggreNum - 2]

    //为 aggregator组分配进程号
    for(j = 0; j < aggreGroupWorkerNum - 1; j ++){
        aggreRank[j] =workerNum + j + 1;
    }
    aggreRank [j] = 0;//MASTER 的 rank号，存在组里末尾

     //构造进程组
    //ori中存各组开头的rank
    //构造 aggreNum 个通信域
    //组里id转换
    int m;
    for(m = 0; m < aggreNum - 1; m ++){
        //MPI_Group group1, group2;
        MPI_Group_incl(world_group, colnum + 1, newranks[m], &group[m]);
        ori[m] = &newranks[m][colnum];//每个通信组的root所在的位置，地址。
        MPI_Comm_create(MPI_COMM_WORLD, group[m], &comm[m]);
        MPI_Group_translate_ranks(world_group, 1, ori[m], group[m], &root[m]);

    }
    MPI_Group_incl(world_group, workerNum - colnum*(aggreNum - 1) + 1, tmprank, &group[m]);
    ori[m] = &tmprank[workerNum - (aggreNum-1)*colnum];
    MPI_Comm_create(MPI_COMM_WORLD, group[m], &comm[m]);
    MPI_Group_translate_ranks(world_group, 1, ori[m], group[m], &root[m]);

    //构造aggreGroup的进程组
        MPI_Group_incl(world_group, aggreGroupWorkerNum, aggreRank, &aggreGroup);
        MPI_Comm_create(MPI_COMM_WORLD, aggreGroup, &aggreComm);
        MPI_Group_translate_ranks(world_group, 1, &aggreRank[aggreGroupWorkerNum - 1], aggreGroup, &aggreRoot);
        //cout<<"aggreRoot = " << aggreRoot << endl;
////////////////////////////////////////////////////


    // Initialize and Register struct EDGE, ELEMCNT information
    // lenAttr：大小为 2 的数组，描述 Edge 中 src和dst 各自对应的 build-in 数据个数
    int          lenAttr[Edge::szAttr] = {1, 1};

    // arrType：大小为 2 的数组，元素是 MPI_UNSIGNED 类型的（0x4c000406)
    MPI_Datatype arrType[Edge::szAttr] = {MPI_UNSIGNED, MPI_UNSIGNED};

    //offsets：大小为 2 的数组，元素是 MPI_Aint 类型的，存储的是 Edge 中 src和dst 的偏移量
    MPI_Aint     offsets[Edge::szAttr];

    // offsets[0]中存储的是 src 在 Edge 结构体中相对于开头的字节偏移量。 应该是0
    offsets[0] = offsetof(Edge, src);
    // offsets[1]中存储的是 dst 的偏移量，应该是 4。
    offsets[1] = offsetof(Edge, dst);

    // MPI_TYPE_EDGE：MPI中可传递的build-in类型数据，相当于一个结构体，
    //包括了 Edge 中变量的个数、每个变量中含几个元素（Edge中的变量是基本类型，所以都只含1个）、每个变量在Edge中的偏移、Edge中每个变量在MPI中对应的数据类型
    MPI_Type_create_struct(Edge::szAttr, lenAttr, offsets, arrType, &MPI_TYPE_EDGE);

    //提交新定义的数据类型 MPI_TYPE_EDGE，提交后才可以使用
    MPI_Type_commit(&MPI_TYPE_EDGE);


    //同理提交关于ElemCnt的新类型，ElemCnt 应是 局部三角形数， vid-count
    arrType[0] = MPI_UNSIGNED;
    arrType[1] = MPI_DOUBLE;
    offsets[0] = offsetof(ElemCnt, vid);
    offsets[1] = offsetof(ElemCnt, cnt);
    MPI_Type_create_struct(ElemCnt::szAttr, lenAttr, offsets, arrType, &MPI_TYPE_ELEMCNT);
    MPI_Type_commit(&MPI_TYPE_ELEMCNT);
}

// Initialize requests and buffers
void MPIIO::init(int lenBuf, int workerNum) {
    commCostDistribute = 0;
    commCostGather = 0;
    ioCPUTime = 0;

    // eBuf： vector<EdgeContainer>
    eBuf.clear();
    MPIIO::lenBuf = lenBuf;
    MPIIO::workerNum = workerNum;

     //@YuMD
     // 分组
    //@bamboo
    // eBuf在不同worker中不一样。
    // 若是当前是master，则eBuf大小为 workerNum， 从 eBuf[0] 到 eBuf[workerNum-1] 存着发往各worker的EdgeContainer
    // 若是当前是worker，则eBuf大小为1，只有 eBuf[0]，存的是本worker上的数据
    if (rank == MPI_MASTER) {
        eBuf.resize(workerNum);
        for (int i = 0; i < workerNum; i++) {
            eBuf[i].init(i);
        }
    } else {
        eBuf.resize(1);
        eBuf[0].init(getWorkerId());
    }
}


bool MPIIO::isMaster() {
    return rank == MPI_MASTER;
}

//bool MPIIO::isActiveWorker()
//{
//    return rank <= workerNum;
//}

MID MPIIO::getWorkerId() {
    return (MID)(rank - 1);
}

int MPIIO::getWorkerNum(){
    return workerNum;
}

//int MPIIO::getAgrregatorNum(){
   // return
//}

long MPIIO::getCommCostDistribute() {
    return commCostDistribute;
}

long MPIIO::getCommCostGather() {
    return commCostGather;

}

void MPIIO::cleanup(int aggNum) {
    for(int m = 0; m < aggNum; m ++){
    if(MPI_GROUP_NULL!=group[m]) MPI_Group_free(&group[m]);
    if(MPI_COMM_NULL!=comm[m]) MPI_Comm_free(&comm[m]);
    }
    MPI_Finalize();

}


// run.cpp中调用此函数实现对edge的广播
// rank = 0，是在MASTER上调用的， 因此 eBuf 的大小为 workerNum
bool MPIIO::bCastEdge(Edge &iEdge, MID dst1, MID dst2) {
    //tempEdge 表示接收到的(src,dst)边
    Edge tmpEdge(iEdge);
    //////////////////////////////////////////////cout<<"bCast中.................   收到的边iEdge.src = " << iEdge.src << "        iEdge.dst = " << iEdge.dst << endl;

    //这个循环实现对 tempEdge 的广播。
    // mit 表示遍历 worker 的索引
    for (int mit = 0; mit < workerNum; mit++) {
           ////////////////////////////////////////////// cout << "bCastEdge.... 循环中  mit = " << mit << endl;
        // eBuf数组，含有workNum个元素
        // eBuf[mit]中存的是第 mit 个 worker 中的 EdgeContainer
        // mit + 1才是对应的workerId，workerNum = hIO.getSzProc()-1。 即输入参数 8，workerNum 的值被赋为7.
        // eBuf[0] ... eBuf[7]中存的是这7个worker中的EdgeContainer
        eBuf[mit].putNext(tmpEdge);
        //cout << "bCastEdge.... 循环中  mit = " << mit << endl;
    }
    commCostDistribute += workerNum;
    //////////////////////////////////////////////cout << "bCastEdge.................................." << endl;

    return true;
}

bool MPIIO::sendEdge(const Edge &iEdge, MID dst){
    Edge tmpEdge(iEdge);
    //@bamboo
    //只发给目标worker
    eBuf[dst].putNext(tmpEdge);
    commCostDistribute += 1;
    return true;
}


// run.cpp run_api中调用
//实现对边的单播
//@YuMD
/*
bool MPIIO::sendEdge2Worker(Edge &iEdge, MID dst,  MPI_Request &iReq, MPI_Status &iSta) { //传过来的是变量iReq iSta，但是可以根据 & 取到地址并改变iReq
    Edge tmpEdge(iEdge);
    //eBuf[dst].putNext(tmpEdge);

    MPI_Isend(&tmpEdge, 1,  MPI_TYPE_EDGE, dst + 1, TAG_STREAM, MPI_COMM_WORLD, &iReq);
    MPI_Wait(&iReq, &iSta);
    commCostDistribute += 1;
    return true;
}
*/

// 对各worker而言，接收来自 MASTER 的边
bool MPIIO::IrecvEdge(Edge *buf, MPI_Request &iReq) {
    return (MPI_SUCCESS == MPI_Irecv(buf, lenBuf, MPI_TYPE_EDGE, MPI_MASTER, TAG_STREAM, MPI_COMM_WORLD, &iReq));

    //waitIOCompletion(iReq);
}

bool MPIIO::IsendEdge(Edge *buf, int mid, MPI_Request &iReq) {
    //////////////////////////cout << "isendedge...." << endl;
    MPI_Isend(buf, lenBuf, MPI_TYPE_EDGE, mid + 1, TAG_STREAM, MPI_COMM_WORLD, &iReq); // mid + 1 才是对应的workerId，将对应EdgeContainer中的缓存(1000条edge)全都发给了对应的worker

    return true;
}

// @bamboo
// 由worker调用，因此只有 eBuf[0] 存自身
bool MPIIO::recvEdge(Edge &oEdge) {
    ////////////////////////cout<<"recvEdge,,,,,,,,,,,  oEdge = " <<oEdge<<endl;
    eBuf[0].getNext(oEdge); // 从worker的EdgeContainer中取一条边
    if (oEdge == END_STREAM) {
        eBuf[0].cleanup();
    }
    return (oEdge != END_STREAM);
}

bool MPIIO::sendEndSignal() {
    Edge signal(END_STREAM);

    for (int mit = 0; mit < workerNum; mit++) {
        eBuf[mit].putNext(signal);
        eBuf[mit].flushSend();
    }
    return true;
}
//@YuMD
//不考虑 localCnt
bool MPIIO::sendCnt_v3_global(double gCnt, int commid, int worker, int aggNum ) {
    clock_t begin = clock();
    double res;
    //@bamboo
    MPI_Reduce(&gCnt, nullptr, 1, MPI_DOUBLE, MPI_SUM, root[commid], comm[commid]);
    //VID maxVId;
    //MPI_Bcast(&maxVId, 1, MPI_UNSIGNED, loc_aggreid, MPI_COMM_WORLD);
    ioCPUTime += double(clock() - begin);
    //float* lCntArr = new float[maxVId + 1];
    //std::fill_n(lCntArr, maxVId + 1, 0.0);
    // it : map<vid, localcnt> lCnt的迭代器
    //unordered_map<VID, float>::const_iterator it;
    //for (it = lCnt.begin(); it != lCnt.end(); it++) {
        //lCntArr[it->first] = it->second;
    //}
    begin = clock();
    //MPI_Reduce(lCntArr, nullptr, maxVId + 1, MPI_FLOAT, MPI_SUM, loc_aggreid, MPI_COMM_WORLD);
    //ioCPUTime += double(clock() - begin);

    //delete lCntArr;
    return true;
}

bool MPIIO::sendCnt2Master_global(double gloCnt, MID arank){
    MPI_Reduce(&gloCnt, nullptr, 1, MPI_DOUBLE, MPI_SUM, aggreRoot, aggreComm);
    return true;
}
bool MPIIO::recvCnt_Maser_global(double &gloCnt){
   int curAgg;
   double empty = 0;
   //MPI_Comm_rank(aggreComm, &curAgg);
   MPI_Reduce(&empty, &gloCnt, 1, MPI_DOUBLE, MPI_SUM, aggreRoot              , aggreComm);
   return true;
}

bool MPIIO::recvCnt_v3_global(double &gCnt, int commid) {
    //TODO. unsinged int -> unsigned long long?

    double empty = 0;
    clock_t begin = clock();
    int curAgg;
    MPI_Comm_rank(comm[commid], &curAgg);//判断本aggregator 在当前通信域中的rank号，存在 curAgg 中
    //////////////////////////////////////////////cout <<"io.cpp.....................   recvCnt中 commid = " << commid << "curAgg = " << curAgg << endl;
    MPI_Reduce(&empty, &gCnt, 1, MPI_DOUBLE, MPI_SUM, curAgg, comm[commid]);
    //MPI_Bcast(&maxVId, 1, MPI_UNSIGNED, MPI_MASTER, MPI_COMM_WORLD);
    ioCPUTime += double(clock() - begin);

    //float* lCntArr = new float[maxVId + 1]; //生成含 maxVId+1 个元素的数组 ICntArr
    //std::fill_n(lCntArr, maxVId + 1, 0.0); // 初始化， ICntArr 各元素初始化为0

    //begin = clock();
    //MPI_Reduce(MPI_IN_PLACE, lCntArr, maxVId + 1, MPI_FLOAT, MPI_SUM, MPI_MASTER, MPI_COMM_WORLD);
   // ioCPUTime += double(clock() - begin);

    //commCostGather = (maxVId + 1) * (getSzProc() - 1);

    //lCnt.insert(lCnt.end(), &lCntArr[0], &lCntArr[maxVId]);
    //delete lCntArr;
    return true;
}





//@bamboo
// worker将结果发给 aggregator 汇总，此处由master 充当
/*
bool MPIIO::sendCnt(double gCnt, unordered_map<VID, float> &lCnt, MID aggreid, MID loc_aggreid_src, MID loc_aggid_dst) {
    clock_t begin = clock();
    double res;


    //@bamboo

    MPI_Reduce(&gCnt, nullptr, 1, MPI_DOUBLE, MPI_SUM, aggreid, MPI_COMM_WORLD);
    //cout << "sendCnt: globalCnt = " << res << endl;


    VID maxVId;
    MPI_Bcast(&maxVId, 1, MPI_UNSIGNED, loc_aggreid, MPI_COMM_WORLD);
    ioCPUTime += double(clock() - begin);


    float* lCntArr = new float[maxVId + 1];
    std::fill_n(lCntArr, maxVId + 1, 0.0);
    // it : map<vid, localcnt> lCnt的迭代器
    unordered_map<VID, float>::const_iterator it;
    for (it = lCnt.begin(); it != lCnt.end(); it++) {
        lCntArr[it->first] = it->second;
    }

    begin = clock();
    MPI_Reduce(lCntArr, nullptr, maxVId + 1, MPI_FLOAT, MPI_SUM, loc_aggreid, MPI_COMM_WORLD);
    ioCPUTime += double(clock() - begin);

    delete lCntArr;
    return true;


}*/
// @bamboo
// aggregator 负责聚合
/*bool MPIIO::recvCnt(VID maxVId, double &gCnt, std::vector<float> &lCnt) {
    //TODO. unsinged int -> unsigned long long?

    double empty = 0;
    clock_t begin = clock();
    MPI_Reduce(&empty, &gCnt, 1, MPI_DOUBLE, MPI_SUM, MPI_MASTER, MPI_COMM_WORLD);
    MPI_Bcast(&maxVId, 1, MPI_UNSIGNED, MPI_MASTER, MPI_COMM_WORLD);
    ioCPUTime += double(clock() - begin);

    float* lCntArr = new float[maxVId + 1]; //生成含 maxVId+1 个元素的数组 ICntArr
    std::fill_n(lCntArr, maxVId + 1, 0.0); // 初始化， ICntArr 各元素初始化为0

    begin = clock();
    MPI_Reduce(MPI_IN_PLACE, lCntArr, maxVId + 1, MPI_FLOAT, MPI_SUM, MPI_MASTER, MPI_COMM_WORLD);
    ioCPUTime += double(clock() - begin);

    commCostGather = (maxVId + 1) * (getSzProc() - 1);

    lCnt.insert(lCnt.end(), &lCntArr[0], &lCntArr[maxVId]);
    delete lCntArr;


    return true;
}*/

double MPIIO::getIOCPUTime() {
    if (rank == MPI_MASTER) {
        double totalIOCPUTime = ioCPUTime;
        for (int i = 0; i < workerNum; i++) {
            totalIOCPUTime += eBuf[i].ioCPUTime;
        }
        return totalIOCPUTime;
    } else {
        return eBuf[0].ioCPUTime;
    }
}
/*
bool MPIIO::sendTime(double compTime) {
    MPI_Reduce(&compTime, nullptr, 1, MPI_DOUBLE, MPI_MAX, glo_aggreid, MPI_COMM_WORLD);
    MPI_Reduce(&compTime, nullptr, 1, MPI_DOUBLE, MPI_SUM, glo_aggreid, MPI_COMM_WORLD);
    return true;
}

bool MPIIO::recvTime(double &compTimeMax, double &compTimeSum) {
    double empty = 0;
    MPI_Reduce(&empty, &compTimeMax, 1, MPI_DOUBLE, MPI_MAX, MPI_MASTER, MPI_COMM_WORLD);
    MPI_Reduce(&empty, &compTimeSum, 1, MPI_DOUBLE, MPI_SUM, MPI_MASTER, MPI_COMM_WORLD);
    return true;

}*/
