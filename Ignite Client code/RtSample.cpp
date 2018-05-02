/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <iostream>
#include <string>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

#include "RtCassTableDataIgnite.hpp"

#include<stdlib.h>
#include<pthread.h>


using namespace ignite;
using namespace cache;

using namespace iothss;
using namespace std;

struct ThreadDataVo
{
	int currThrd;
	long maxlimit;
	long start;
	int numberOfSubs;
	ThreadDataVo()
	{
		start = 0;
		currThrd = 0;
		maxlimit = 0;
		numberOfSubs=0;
	}
};

struct timespec tstart={0,0}, tend={0,0};

volatile int g_count = 0;

IgniteConfiguration cfg;
Ignite grid;


int noOfSubs=0;
int appThread = 0;
char *ip;
char* keyspace;
int  counter=0;

void *threadFunc(void *arg)
{
	//printf("\n[%s::%d] Inside threadFunc \n",__FILE__,__LINE__);

	ThreadDataVo  l_obj = *(ThreadDataVo *)arg;
	long imsi =  l_obj.start ;
	long threadExpiration= l_obj.numberOfSubs;
	Cache<string, RtCassTableData> cache = grid.GetCache<string, RtCassTableData>("cache1");

	while(threadExpiration > 0)
	{
		RtCassTableData l_epcdata;
		l_epcdata=RtCassTableData(10);
		char l_imsi[IMSI_SIZE] = {0};
                sprintf(l_imsi,"%ld",imsi);
	

		
		strncpy(l_epcdata.m_imsi,l_imsi,IMSI_SIZE);

		IgniteError l_error;
		RtCassTableData dataFromCache = cache.Get(l_imsi,l_error);

		imsi++;
		threadExpiration--;

		__atomic_add_fetch (&g_count , 1 , __ATOMIC_SEQ_CST);
	}
	pthread_exit(NULL);

}


int main(int argc, char *argv[])
{

	if(argc < 3)
	{
		printf("\n Usage: [./bin/rtDb] [appThread] [No_Of_Rows] \n");
		exit(0);
	}


	sscanf(argv[1],"%d",&appThread);
	sscanf(argv[2],"%d",&noOfSubs);


	try
	{
		//IgniteConfiguration cfg;
		cfg.springCfgPath = "/home/ignite/apache-ignite-fabric-2.4.0-bin/config/cassandra-client-config.xml";


		grid = Ignition::Start(cfg);       // Start a node.
		Cache<string, RtCassTableData> cache1 = grid.GetCache<string, RtCassTableData>("cache1"); // Get cache instance.

		cout<<"\n sizeof(RtCassTableData)-->"<<sizeof(RtCassTableData)<<endl;
		int32_t l_noOfKeyCache = cache1.LocalSize();
		cout<<"\n l_noOfKeyCache -->"<<l_noOfKeyCache<<endl;

		ThreadDataVo *l_threadVo= new ThreadDataVo[appThread];
		long rangeStart= 100000000000001;
		int noOfSubscriber=noOfSubs; 
		int noOsSubsCperThrd =  noOfSubscriber / appThread;


		clock_gettime(CLOCK_MONOTONIC, &tstart);
		pthread_t threads[appThread];
		static int thNo = 1;
		for(int i=0; i<appThread;i++)
		{
			l_threadVo[i].currThrd = i;
			l_threadVo[i].start =  rangeStart + i * noOsSubsCperThrd;
			l_threadVo[i].maxlimit = l_threadVo[i].start + noOsSubsCperThrd;
			l_threadVo[i].numberOfSubs = noOsSubsCperThrd;

			int rc = pthread_create(&threads[i], NULL, threadFunc,(void *)&l_threadVo[i]);
			if (rc)
			{
				cout << "Error:unable to create thread," << rc << endl;
				exit(-1);
			}
			thNo++;
		}


		for(int i=0; i<appThread;i++)
		{
			pthread_join(threads[i], NULL);
		}

		clock_gettime(CLOCK_MONOTONIC, &tend);
		double diff_time = ((double)tend.tv_sec + 1.0e-9*tend.tv_nsec) - ((double)tstart.tv_sec + 1.0e-9*tstart.tv_nsec);

		cout<<"\n TPS-->"<<g_count/diff_time<<endl;
		cout<<"\n RequestNoOfRows-->"<<g_count<<endl;
		
		
		// Stop node.
		Ignition::StopAll(false);

	}
	catch (IgniteError& err)
	{
		std::cout << "An error occurred: " << err.GetText() << std::endl;

		return err.GetCode();
	}

	std::cout << std::endl;
	std::cout << ">>> Example finished, press 'Enter' to exit ..." << std::endl;
	std::cout << std::endl;

	std::cin.get();

	return 0;
}
