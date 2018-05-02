#ifndef __RT_CONNECTION_OBJ_HPP__
#define __RT_CONNECTION_OBJ_HPP__

#include<cassandra.h>
#include<stdio.h>
#include<iostream>
using namespace std;

class RtConnectionObj
{
		static RtConnectionObj * m_instance;
	public :

		static CassCluster* ms_cluster;
		static CassSession* ms_session;
		

		static RtConnectionObj * rtReturnInstance();
		static CassSession* rtGetSession();

	//	static void rtSetSession(CassSession *)

		static void initilizeSession();

		RtConnectionObj();
		~RtConnectionObj();
		
};
#endif // __RT_CONNECTION_OBJ_HPP__
