#ifndef _OPTION_HPP_
#define _OPTION_HPP_

#include "macro.hpp"
#include <iostream>
#include <getopt.h>
#include <strings.h>

using namespace std;

enum optionEnum {
	SIZE_BUDGET, SIZE_TRIAL, SIZE_TOLERANCE, SIZE_AGGREGATOR //@bamboo..........................................
};

class Option {
_PRIVATE:

public:
	static char* 	outPath;
	static char*	inFileName;

	static double		tolerance;
	static unsigned int budget;
	static unsigned int trial;

	//@YuMD
	//
	static  unsigned int aggregator;

	static bool parse(int argc, char **argv, int &aggNum);
	static void print();

};

#endif // _OPTION_HPP_
