#ifndef __URLENCODE_H__
#define __URLENCODE_H__

#include <iostream>
#include <string>

std::string char2hex( char dec );
std::string urlencode( const std::string &c );

#endif // __URLENCODE_H__