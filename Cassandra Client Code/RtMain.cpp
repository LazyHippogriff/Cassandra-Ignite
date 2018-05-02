#include "RtConnectionObj.hpp"

#include "RtCassStaticDataDao.hpp"
#include <atomic>


#include<stdlib.h>
#include<pthread.h>
using namespace std;

int g_Found=0;
int g_notFound=0;

std::atomic<int> fetch_count(0);
std::atomic<int> g_count(0);

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



int noOfSubs=0;
int appThread = 0;
char *ip;
char* keyspace;
int  counter=0;

struct timespec tstart={0,0}, tend={0,0};




void *threadFunc(void *arg)
{
	ThreadDataVo  l_obj = *(ThreadDataVo *)arg;
	long imsi =  l_obj.start ;	
	long threadExpiration= l_obj.numberOfSubs;

	while(threadExpiration > 0)
	{
		RtCassTableDataVo EpcData;
		char l_imsi[IMSI_SIZE] = {0};
		
		sprintf(l_imsi,"%ld",imsi);
		strncpy(EpcData.m_imsi,l_imsi,IMSI_SIZE);

	RtReturnCodes l_retCode = RtCassTableDataDao::rtFetchTableData(EpcData);
		if(l_retCode!=RT_DB_SUCCESS)
			printf("\n[%s::%d] Error",__FILE__,__LINE__);

		imsi++;
		threadExpiration--;

		g_count.fetch_add(1);
	}
	pthread_exit(NULL);

}


int main(int argc, char *argv[])
{
	if(argc < 7)
	{
		printf("\n Usage: [./bin/rtDb] [appThread] [No_Of_Rows] [IP] [KEYSPACE_NAME] [IMSI] [Insert]\n");
		exit(0);
	}

	sscanf(argv[1],"%d",&appThread);
	sscanf(argv[2],"%d",&noOfSubs);
	ip=argv[3];
	keyspace=argv[4];
	RtConnectionObj::initilizeSession();

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


	CassFuture* close_future = NULL;
	close_future = cass_session_close(RtConnectionObj::ms_session);
	cass_future_wait(close_future);
	if(close_future != NULL)
		cass_future_free(close_future);


	clock_gettime(CLOCK_MONOTONIC, &tend);
	double diff_time = ((double)tend.tv_sec + 1.0e-9*tend.tv_nsec) - ((double)tstart.tv_sec + 1.0e-9*tstart.tv_nsec);	

	cout<<"\n TPS-->"<<fetch_count/diff_time<<endl;
	cout<<"\n Found-->"<<fetch_count<<endl;
	cout<<"\n RequestNoOfRows-->"<<g_count<<endl;

	clock_gettime(CLOCK_MONOTONIC, &tend);

	if(RtConnectionObj::ms_session != NULL)
	{
		printf("[%s::%d] ### cass_session_free  ### \n" , __FILE__,__LINE__);
		cass_session_free(RtConnectionObj::ms_session);
		RtConnectionObj::ms_session = NULL;
	}


	if(RtConnectionObj::ms_cluster != NULL)
	{
		printf("[%s::%d]  ### cass_cluster_free ### \n" , __FILE__,__LINE__);
		cass_cluster_free(RtConnectionObj::ms_cluster);
		RtConnectionObj::ms_cluster = NULL;
	}

	return 0;
}
