from __future__ import print_function

import StringIO
import csv
import json
import os
import zlib

from time import strftime, gmtime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from jinja2 import Template

import boto3
import botocore
import concurrent.futures

__author__ = 'DWP DataWorks'
__date__ = '01/08/2018'
__version__ = '1.0'

# Get Lambda environment variables
region = os.environ['REGION']
max_threads = os.environ['MAX_THREADS']

# Initialize clients
s3 = boto3.client('s3', region_name=region)
ses = boto3.client('ses', region_name=region)
send_errors = []
mime_message_text = ''
mime_message_html = ''


def current_time():
    return strftime("%Y-%m-%d %H:%M:%S UTC", gmtime())


def mime_email(subject, from_address, to_address, text_message=None, html_message=None):
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = from_address
    msg['To'] = to_address
    if text_message:
        msg.attach(MIMEText(text_message, 'plain'))
    if html_message:
        msg.attach(MIMEText(html_message, 'html'))

    return msg.as_string()


def send_mail(from_address, to_address, message):
    global send_errors
    try:
        response = ses.send_raw_email(
            Source=from_address,
            Destinations=[
                to_address,
            ],
            RawMessage={
                'Data': message
            }
        )
        if not isinstance(response, dict):  # log failed requests only
            send_errors.append('%s, %s, %s' % (current_time(), to_address, response))
    except botocore.exceptions.ClientError as e:
        send_errors.append('%s, %s, %s, %s' %
                           (current_time(),
                            to_address,
                            ', '.join("%s=%r" % (k, v) for (k, v) in e.response['ResponseMetadata'].iteritems()),
                            e.message))


def lambda_handler(event, context):
    global send_errors
    global mime_message_text
    global mime_message_html
    try:
        # Read the uploaded csv file from the bucket into python dictionary list
        print(event)
        message = json.loads(event['Records'][0]['Sns']['Message'])['ses_mailer']
        bucket = message['bucket']
        mailing_list = message['mailing_list']
        text_message_file = message['plain_text_template']
        html_message_file = message['html_template']
        response = s3.get_object(Bucket=bucket, Key=mailing_list)
        body = zlib.decompress(response['Body'].read(), 16+zlib.MAX_WBITS)
        reader = csv.DictReader(StringIO.StringIO(body),
                                fieldnames=['from_address', 'to_address'])

        # Read the message files
        try:
            response = s3.get_object(Bucket=bucket, Key=text_message_file)
            mime_message_text = response['Body'].read()
            t = Template(mime_message_text)
            mime_message_text = t.render(something="World")
        except:
            mime_message_text = None
            print('Failed to read text message file. Did you upload %s?' % text_message_file)
        try:
            response = s3.get_object(Bucket=bucket, Key=html_message_file)
            mime_message_html = response['Body'].read()
            t = Template(mime_message_html)
            mime_message_html = t.render(something="World")
        except:
            mime_message_html = None
            print('Failed to read html message file. Did you upload %s?' % html_message_file)

        if not mime_message_text and not mime_message_html:
            raise ValueError('Cannot continue without a text or html message file.')

        # Send in parallel using several threads
        e = concurrent.futures.ThreadPoolExecutor(max_workers=max_threads)
        for row in reader:
            from_address = row['from_address'].strip()
            to_address = row['to_address'].strip()
            subject = event['Records'][0]['Sns']['Subject']
            message = mime_email(subject, from_address, to_address, mime_message_text, mime_message_html)
            e.submit(send_mail, from_address, to_address, message)
        e.shutdown()
    except Exception as e:
        print(e.message + ' Aborting...')
        raise e

    print('Send email complete.')

    # Upload errors if any to S3
    if len(send_errors) > 0:
        try:
            result_data = '\n'.join(send_errors)
            logfile_key = key.replace('.csv.gz', '') + '_error.log'
            response = s3.put_object(Bucket=bucket, Key=logfile_key, Body=result_data)
            if 'ResponseMetadata' in response.keys() and response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print('Send email errors saved in s3://%s/%s' % (bucket, logfile_key))
        except Exception as e:
            print(e)
            raise e
        # Reset publish error log
        send_errors = []


if __name__ == "__main__":
    json_content = json.loads(open('event.json', 'r').read())
    lambda_handler(json_content, None)