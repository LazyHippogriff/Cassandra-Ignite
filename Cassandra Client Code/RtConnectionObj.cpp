#include"RtConnectionObj.hpp"
#include "stdlib.h"

CassCluster *RtConnectionObj::ms_cluster = NULL;;

CassSession *RtConnectionObj::ms_session = NULL;
RtConnectionObj *RtConnectionObj::m_instance = NULL;

extern int threadio;
extern char* ip;
extern char* keyspace;

RtConnectionObj::RtConnectionObj()
{
	printf("[%s::%d] -- > RtConnectionObj Constructor  \n" , __FILE__,__LINE__);
}

/*******************************************************************************
*
* FUNCTION NAME : rtReturnInstance
*
- DESCRIPTION   : The Function Returns The Instance of the Class
*
*
* INPUT                 :void

* OUTPUT                :
*
* RETURN                :void
*
*******************************************************************************/
RtConnectionObj * RtConnectionObj::rtReturnInstance()
{
	try
	{
		if(m_instance == NULL)
			m_instance = new RtConnectionObj();

		return m_instance;

	}
	catch(const std::exception& e)
	{
		printf("\n [%s::%d] --> Error in constructing the Instance of the class %s\n",__FILE__,__LINE__,e.what());
		fflush(stdout);
		//exit(0);

	}
	catch (...)
	{
		printf("\n [%s::%d] --> Error in constructing the Instance of the class \n",__FILE__,__LINE__);
		fflush(stdout);
		//exit(0);
	}
}// end of rtReturnInstance


/*******************************************************************************
*
* FUNCTION NAME : rtReturnInstance
*
- DESCRIPTION   : The Function Returns The Instance of the Class
*
*
* INPUT                 :void

* OUTPUT                :
*
* RETURN                :void
*
*******************************************************************************/
CassSession* RtConnectionObj::rtGetSession()
{
	try{
		if(ms_session != NULL)
			return ms_session;
	}
	catch(...)
	{
		printf("\n[%s::%d] Error in returning Cass Session\n");
	}
}





// Initialize the Session
void RtConnectionObj::initilizeSession()
{
	ms_cluster = cass_cluster_new();

	cass_cluster_set_max_connections_per_host ( ms_cluster, 20 );
	cass_cluster_set_queue_size_io ( ms_cluster, 102400*1024 );
	cass_cluster_set_pending_requests_low_water_mark(ms_cluster, 50000);
	cass_cluster_set_pending_requests_high_water_mark(ms_cluster, 100000);
	cass_cluster_set_write_bytes_low_water_mark(ms_cluster, 100000 * 2024);
	cass_cluster_set_write_bytes_high_water_mark(ms_cluster, 100000 * 2024);
	cass_cluster_set_max_requests_per_flush(ms_cluster, 10000);

	cass_cluster_set_request_timeout ( ms_cluster, 12000 );
	cass_cluster_set_connect_timeout (ms_cluster, 60000);

	//  cass_cluster_set_connection_heartbeat_interval(cluster, 60);
	/* Disable heartbeat requests */
	
	cass_cluster_set_connection_heartbeat_interval(ms_cluster, 60);
	cass_cluster_set_connection_idle_timeout(ms_cluster, 120);

	
	//cass_cluster_set_max_concurrent_creation(ms_cluster,2);
	cass_cluster_set_core_connections_per_host(ms_cluster,1);
	cass_cluster_set_num_threads_io(ms_cluster,10);
	
	//cass_cluster_set_max_concurrent_requests_threshold(ms_cluster,10000);
	
	ms_session = cass_session_new();
	cass_cluster_set_contact_points(ms_cluster, (const char *)ip);
	CassFuture* connect_future = cass_session_connect_keyspace(ms_session, ms_cluster,keyspace);
	 
	/* This operation will block until the result is ready */
	CassError rc = cass_future_error_code(connect_future);
	if(rc != CASS_OK)
	{	
		printf("[%s::%d] Connect result: %s  \n" , __FILE__,__LINE__,cass_error_desc(rc));
		exit(0);
	}


	cass_future_free(connect_future);
	printf("[%s::%d] Connect Succesfull...   \n" , __FILE__,__LINE__);
}



RtConnectionObj::~RtConnectionObj()
{
	if(ms_cluster != NULL)
	{
		printf("[%s::%d]  ### cass_cluster_free ### \n" , __FILE__,__LINE__);
		cass_cluster_free(ms_cluster);
		ms_cluster=NULL;
	}

	if(ms_session != NULL)
	{
		printf("[%s::%d] ### cass_session_free  ### \n" , __FILE__,__LINE__);
		cass_session_free(ms_session);
		ms_session=NULL;
	}
}
