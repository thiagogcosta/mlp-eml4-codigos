import json
import pymysql
import logging
import sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def save_to_rds(item):
    rds_host = "rds-instance-endpoint"
    name = "db_username"
    password = "db_password"
    db_name = "db_name"

    try:
        logger.info('Connecting')
        conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
        logger.info('Connected')
    except pymysql.MySQLError as e:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
        logger.error(e)
        sys.exit()

    logger.info('Saving Passenger Data')
    with conn.cursor() as cur:
        sql = f'''insert into titanic_passengers (passenger_id, survived, pclass, name, sex, age, sibsp, parch, ticket, fare, cabin, embarked) values ({item['PassengerId']}, "{item['Survived']}", "{item['Pclass']}", "{item['Name']}", "{item['Sex']}", "{item['Age']}", "{item['SibSp']}", "{item['Parch']}", "{item['Ticket']}", "{item['Fare']}", "{item['Cabin']}", "{item['Embarked']}")'''
        cur.execute(sql)
        conn.commit()
    conn.commit()
    logger.info('Data saved')


def lambda_handler(event, context):
    for record in event['Records']:
        payload = record["body"]
        new_payload = json.loads(payload)
        save_to_rds(new_payload)