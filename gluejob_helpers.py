import sys
import time
import logging
import boto3
logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

def trigger_gluejob(job_name, options, worker_type="Standard", num_workers=None, wait_completion=True):
    client = boto3.client('glue')
    #Starting a job
    logger.info('starting job '+job_name)
    if num_workers is None:
        start = client.start_job_run(JobName=job_name, Arguments=options)
    else:
        start = client.start_job_run(
            JobName=job_name,
            Arguments=options,
            WorkerType=worker_type,
            NumberOfWorkers=num_workers
        )

    if wait_completion is False:
        logger.info(start)
        return

    # get job status for tracked job run id
    response = client.get_job_run(
        JobName = job_name,
        RunId = start["JobRunId"]
    )
    completed_state = 'SUCCEEDED'
    # get ID for tracked job names and job run ids
    logger.info(start)
    job_run_id = start['JobRunId']
    job_status = response['JobRun']['JobRunState']
    while (job_status != completed_state):
        logger.info('JOB_NAME: '+job_name+', job_status: '+ job_status)
        if job_status in ["STOPPED", "FAILED", "ERROR", "TIMEOUT"]:
            logger.info('Failed with error: '+ response['JobRun']['ErrorMessage'])
            logger.info('Exiting the pipeline')
            sys.exit('The EIL source job '+job_name+' failed with error: '+response['JobRun']['ErrorMessage'])
        logger.info('Waiting 1 minute...')
        time.sleep(60)
        response = client.get_job_run(
            JobName = job_name,
            RunId = job_run_id
        )
        job_status = response['JobRun']['JobRunState']
    else:
        logger.info('Execution time: '+ str(response['JobRun']['ExecutionTime'])+' sec, ')

def run_crawler(crawler):
    logger.info(f"Starting crawler: {crawler}")
    client = boto3.client("glue")
    start = client.start_crawler(Name=crawler)
    response = client.get_crawler(Name=crawler)
    completed_state = 'READY'
    crawler_state = response["Crawler"]["State"]
    while (crawler_state != completed_state):
        logger.info(f"crawler_state: {crawler_state}")
        if crawler_state not in ('READY','RUNNING','STOPPING'):
            logger.info(f"Failed with error: {response['Crawler']['LastCrawl']}")
            logger.info('Exiting the pipeline')
            raise SystemError(f"The crawler {crawler} failed with error: {response['Crawler']['LastCrawl']['ErrorMessage']}")
        logger.info('Waiting 1 minutes to check crawler_state again...')
        time.sleep(60)
        response = client.get_crawler(
            Name = crawler
        )
        crawler_state = response["Crawler"]["State"]
    else:
        logger.info(f"crawler_state: {crawler_state}")
        logger.info(f"Last Crawl Status: {response['Crawler']['LastCrawl']['Status']}")

