#include "worker.hpp"

Worker::Worker(int k, unsigned int seed): k(k), n(0), global(0), generator(seed), distribution(0.0, 1.0) {
	srand(seed+time(NULL));
	samples.reserve(k);
}

void Worker::updateCnt(int opt,const Edge &iEdge){

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
			if(opt == 1){
				if (node.find(neighbor) == node.end()) {
					node[neighbor] = 1.0;
				} else {
					node[neighbor] += 1.0;
				}
			}else{
				if (node.find(neighbor) != node.end()) {
					node[neighbor] -= 1.0;
					node[neighbor] = node[neighbor] > 0 ? node[neighbor] : 0;
				}
			}
		}
	}
	if(opt == 1 ){
		if(countSum > 0) {
			if (node.find(src) == node.end()) {
				node[src] = (float)countSum;
			} else {
				node[src] += (float)countSum;
			}
	
			if (node.find(dst) == node.end()) {
				node[dst] = (float)countSum;
			} else {
				node[dst] += (float)countSum;
			}
			
			global += countSum;
		}
	}else{
		if(countSum > 0) {
				node[src] -= (float)countSum;
				node[src] = node[src] > 0 ? node[src] : 0;

				node[dst] -= (float)countSum;
				node[dst] = node[dst] > 0 ? node[dst] : 0;
		} 
		global -= countSum;
		global = global > 0 ? global : 0;
	}
	return;
}

int Worker::deleteEdge() {
	int index = rand() % k;
	Edge removedEdge = samples[index];
	updateCnt(-1, removedEdge);  //////////////////////////////// 
	nodeToNeighbors[removedEdge.src].erase(removedEdge.dst);
	nodeToNeighbors[removedEdge.dst].erase(removedEdge.src);
	return index;
}
void Worker::processEdge(const Edge &iEdge){

	VID src = iEdge.src;
	VID dst = iEdge.dst;

	if(src == dst) { //ignore self loop
		return;
	}
	
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
			updateCnt(1,iEdge);
		}else {
			int index = deleteEdge();
			samples[index] = iEdge;
			updateCnt(1,iEdge);
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
void Worker::finalCnt(){
	globalCnt = global * weight(); //全局三角形
	unordered_map<VID, float>::const_iterator it;
    for (it = node.begin(); it != node.end(); it++) {
        nodeToCnt[it->first] = it->second * weight();
    }
	return;
}

double  Worker::weight(){
	double curSampleNum = n <= k ? n : k; // curSampleNum = k 
	double prob = (curSampleNum / n * (curSampleNum - 1) / (n - 1) * (curSampleNum - 2) / (n - 2));
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
