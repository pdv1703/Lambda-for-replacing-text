import boto3
import mysql.connector

def lambda_handler(event, context):

    s3_resource = boto3.resource('s3')
    s3_client = boto3.client('s3')

    bucket = 'bucket-for-lambda-testing'
    replace_template = 'replace_template.txt'
    count_of_replaced_items = 0

    my_bucket = s3_resource.Bucket(bucket)

    try:
        s3_resource.Bucket(bucket).download_file(replace_template, str("/tmp/" + replace_template))
        for item in my_bucket.objects.all():
            changed_in_the_past = False
            for file in my_bucket.objects.all():
                if (str(item.key) + ".replaced") == str(file.key) or str(item.key).endswith(".replaced") is True:
                    changed_in_the_past = True
            if (changed_in_the_past is False) and (item.key != replace_template):
                file_for_replacing = s3_client.get_object(Bucket=item.bucket_name, Key=item.key)
                text_for_replace = file_for_replacing['Body'].read().decode('utf-8')

                for t in open(str("/tmp/" + replace_template)).readlines():
                    replace_parameters = t.strip('\n').strip('\r').split(' ')
                    text_for_replace = text_for_replace.replace(replace_parameters[0], replace_parameters[1])
                    count_of_replaced_items = count_of_replaced_items + text_for_replace.count(replace_parameters[1])

                local_file_name = "/tmp/" + str(item.key) + ".replaced"
                file = open(local_file_name, "a+", encoding='utf-8')
                file.write(text_for_replace)
                file.close()

                s3_client.upload_file(local_file_name, item.bucket_name, str(str(item.key) + ".replaced"))
        print(count_of_replaced_items)
        if count_of_replaced_items != 0:
            insert_to_db(count_of_replaced_items)

    except Exception as e:
        print(e)
        raise e


def insert_to_db(count):
    try:
        db_user = 'lambda'
        db_user_pass = 'TesTLaMBDa'
        db_host = 'test-db-for-lambda.co3fhen34njr.eu-central-1.rds.amazonaws.com'
        db_port = "5432"
        db_database_name = 'lambda'
        cnx = mysql.connector.connect(
            user=db_user,
            password=db_user_pass,
            host=db_host,
            port=db_port,
            database=db_database_name)
        stat_cursor = cnx.cursor()
        query_to_db = (
            """INSERT INTO test_lambda_result(count)
            VALUES
            (""" + str(count) + """);"""
        )
        stat_cursor.execute(query_to_db)
        cnx.commit()
        stat_cursor.close()
        cnx.close()

    except Exception as e:
        print(e)
        raise e
