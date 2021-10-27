#ifndef _OPTION_HPP_
#define _OPTION_HPP_

#include "macro.hpp"
#include <iostream>
#include <getopt.h>
#include <strings.h>

using namespace std;

enum optionEnum {
	SIZE_BUDGET, SIZE_TRIAL, SIZE_TOLERANCE
};

class Option {
_PRIVATE:

public:
	static char* 	outPath; //输出目录 
	static char*	inFileName; //输入文件名 

	static double		tolerance; //时间 
	static unsigned int budget; //采样集大小 
	static unsigned int trial; //实验次数 

	static bool parse(int argc, char **argv);
	static void print();

};

#endif // _OPTION_HPP_
