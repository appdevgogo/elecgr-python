import time
import datetime
import boto3
import logging
import logging.handlers
import boto.ec2
import boto.utils

def init():
    global start_time, logger, region

    region = 'ap-northeast-2'

    logger = logging.getLogger('logging test')
    logger.setLevel(logging.DEBUG)

    fileHandler = logging.FileHandler('./pylog/my.log')
    streamHandler = logging.StreamHandler()

    logger.addHandler(fileHandler)
    logger.addHandler(streamHandler)

    logger.info('logging test')

    start_time = time.time()
    logger.info(datetime.datetime.now())

    stop_ec2()

def stop_ec2():

    conn = boto.ec2.connect_to_region(region)
    my_id = boto.utils.get_instance_metadata()['instance-id']
    logger.info(' stopping EC2 :' + str(my_id))
    conn.stop_instances(instance_ids=[my_id])

init()