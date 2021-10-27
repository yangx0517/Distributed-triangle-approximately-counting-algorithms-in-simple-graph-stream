#include "worker.hpp"

Worker::Worker(int k, unsigned int seed): k(k), n(0), globalCnt(0), generator(seed), distribution(0.0, 1.0) {
	srand(seed+time(NULL));
	samples.reserve(k);
}

void Worker::updateCnt(const Edge &iEdge){

	VID src = iEdge.src;
	VID dst = iEdge.dst;

	if(nodeToNeighbors.find(src) == nodeToNeighbors.end() || nodeToNeighbors.find(dst) == nodeToNeighbors.end()) {
		return;
	}

    if(nodeToNeighbors[src].size() > nodeToNeighbors[dst].size()) {
        VID temp = dst;
        dst = src;
        src = temp;
    }

	std::set<VID> &srcMap = nodeToNeighbors[src];
	std::set<VID> &dstMap = nodeToNeighbors[dst];

	double countSum = 0;
	std::set<VID>::iterator srcIt;
	for (srcIt = srcMap.begin(); srcIt != srcMap.end(); srcIt++) {
		VID neighbor = *srcIt;
		if (dstMap.find(neighbor) != dstMap.end()) {
			countSum += 1.0;
			if (nodeToCnt.find(neighbor) == nodeToCnt.end()) {
				nodeToCnt[neighbor] = weight();
			} else {
				nodeToCnt[neighbor] += weight();
			}
		}
	}

	if(countSum > 0) {
		if (nodeToCnt.find(src) == nodeToCnt.end()) {
			nodeToCnt[src] = (float)countSum * weight();
		} else {
			nodeToCnt[src] += (float)countSum * weight();
		}

		if (nodeToCnt.find(dst) == nodeToCnt.end()) {
			nodeToCnt[dst] = (float)countSum * weight();
		} else {
			nodeToCnt[dst] += (float)countSum * weight();
		}
		
		globalCnt+= countSum * weight();
	}
	
	//cout <<  "triangle:" << globalCnt << endl;
	return;
}

int Worker::deleteEdge() {
	int index = rand() % k;
	Edge removedEdge = samples[index];
	nodeToNeighbors[removedEdge.src].erase(removedEdge.dst);
	nodeToNeighbors[removedEdge.dst].erase(removedEdge.src);
	return index;
}
void Worker::processEdge(const Edge &iEdge, int workerNum, int id, int m_pHash){

	VID src = iEdge.src;
	VID dst = iEdge.dst;
	
	VID minnode = min(src, dst);
    VID maxnode = max(src, dst);

    //������( src, dst ) ӳ�䵽��0 ~ workerNum - 1 �У�ÿ��worker�ж��Ƿ��ɱ�worker�ɼ�
    VID hashValue = iEdge.m;
    //cout << "u:" << src << "  v:" << dst << " hash:" << hashValue << endl;
    
	if(src == dst) { //ignore self loop
		return;
	}
		
	updateCnt(iEdge);	

	if(hashValue == id){
		n++;
		bool isSampled = false;
		if(n <= k) { // always sample
			isSampled = true;
		}else {
			if(distribution(generator) < k * 1.0 / (n)) {
				isSampled = true;
			}
		}
	
		if(isSampled) {
			if(nodeToNeighbors.find(src)==nodeToNeighbors.end()) {
				nodeToNeighbors[src] = std::set<VID>();
			}
			nodeToNeighbors[src].insert(dst);
	
			if(nodeToNeighbors.find(dst)==nodeToNeighbors.end()) {
				nodeToNeighbors[dst] = std::set<VID>();
			}
			nodeToNeighbors[dst].insert(src);
		 
			if(n <= k) {
				samples.push_back(Edge(iEdge));	
			}else {
				int index = deleteEdge();
				samples[index] = iEdge;
			}
		}	
	}
	return;

}
//void Worker::processEdge(const Edge &iEdge){
//
//	VID src = iEdge.src;
//	VID dst = iEdge.dst;
//
//	if(src == dst) { //ignore self loop
//		return;
//	}
//
//	updateCnt(iEdge); //count triangles involved
//
//	bool isSampled = false;
//	if(n < k) { // always sample
//		isSampled = true;
//	}
//	else {
//
//		if(distribution(generator) < k / (1.0+n)) {
//			isSampled = true;
//		}
//	}
//
//	if(isSampled) {
//
//
//		if(n < k) {
//			samples.push_back(Edge(iEdge));
//		}
//
//		else {
//			int index = deleteEdge();
//			samples[index] = iEdge;
//		}
//
//		if(nodeToNeighbors.find(src)==nodeToNeighbors.end()) {
//			nodeToNeighbors[src] = std::set<VID>();
//		}
//		nodeToNeighbors[src].insert(dst);
//
//		if(nodeToNeighbors.find(dst)==nodeToNeighbors.end()) {
//			nodeToNeighbors[dst] = std::set<VID>();
//		}
//		nodeToNeighbors[dst].insert(src);
//	}
//
//	n++;
//
//	return;
//
//}


double  Worker::weight(){
	double curSampleNum = n <= k  ? n : k; // curSampleNum = k 
	double prob = (curSampleNum / n * (curSampleNum - 1) / (n - 1));
	return 1.0 / prob; 
}
double  Worker::getGlobalCnt()
{
	return globalCnt;
}

std::unordered_map<VID, float> & Worker::getLocalCnt()
{
	return nodeToCnt;
}
