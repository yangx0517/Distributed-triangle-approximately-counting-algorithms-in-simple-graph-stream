#ifndef _OPTION_HPP_
#define _OPTION_HPP_

#include "macro.hpp"
#include <iostream>
#include <getopt.h>
#include <strings.h>

using namespace std;

enum optionEnum {
	SIZE_BUDGET, SIZE_TRIAL, SIZE_TOLERANCE, SIZE_THETA
};

class Option {
_PRIVATE:

public:
	static char* 	outPath; //���Ŀ¼ 
	static char*	inFileName; //�����ļ��� 

	static double		tolerance; //ʱ�� 
	static double		theta; //ʱ�� 
	static unsigned int budget; //��������С 
	static unsigned int trial; //ʵ����� 

	static bool parse(int argc, char **argv);
	static void print();

};

#endif // _OPTION_HPP_
