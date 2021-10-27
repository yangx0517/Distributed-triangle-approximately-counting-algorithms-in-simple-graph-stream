#include "source.hpp"

Source::Source(): maxVId(128) {

}

VID Source::getMaxVId() {
    return maxVId;
}

/**
 *
 * @param iEdge input edge
 * @param oDstMID1 destination machine 1
 * @param oDstMID2 destination machine 2
 * @return whether to broadcast
 */

//只实现了判断src 和 dst 哪个更大，maxVid = 二者中更大的那个。
bool Source::processEdge(Edge &iEdge, MID &oDstMID1, MID &oDstMID2) {

    VID src = iEdge.src;
    VID dst = iEdge.dst;
    if (src > maxVId) {
        maxVId = src;
    }
    if (dst > maxVId) {
        maxVId = dst;
    }
    return true;
}

