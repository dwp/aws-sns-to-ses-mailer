from __future__ import print_function

import csv
import json
from collections import namedtuple
import os
import sys
import zlib
import logging

from time import strftime, gmtime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from jinja2 import Template

import boto3
from botocore.exceptions import ClientError
import concurrent.futures

__author__ = 'DWP DataWorks'
__date__ = '01/08/2018'
__version__ = '1.0'

# Get Lambda environment variables
region = os.environ['REGION']
max_threads = int(os.environ['MAX_THREADS'])
from_domain = os.environ['SENDING_DOMAIN']

# Initialise logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.getLevelName(os.environ['LOG_LEVEL'].upper()))
logging.basicConfig(stream=sys.stdout,
                    format='%(asctime)s %(levelname)s %(module)s '
                           '%(process)s[%(thread)s] %(message)s')
logger.info("Logging at {} level".format(os.environ['LOG_LEVEL']))

# Initialise clients
boto3.setup_default_session(profile_name='default')
s3 = boto3.client('s3', region_name=region)
ses = boto3.client('ses', region_name=region)
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
            logger.error('%s, Error sending to: %s, %s' % (current_time(), to_address, response))
    except ClientError as e:
        logger.error('%s, Error sending to: %s, %s, %s' %
                     (current_time(),
                      to_address,
                      ', '.join("%s=%r" % (k, v) for (k, v) in e.response['ResponseMetadata'].iteritems()),
                      e))


def get_parameters(event):
    print(event)
    message = json.loads(event['Records'][0]['Sns']['Message'])

    return message['ses_mailer']


def lambda_handler(event, context):

    global mime_message_text
    global mime_message_html
    try:
        args = get_parameters(event)

        if args['recipients']:
            recipients = args['recipients']
        else:
            recipients = []

        # Read the uploaded csv file from the bucket into python dictionary list
        if args['mailing_list']:
            response = s3.get_object(Bucket=args['bucket'], Key=args['mailing_list'])
            body = zlib.decompress(response['Body'].read(), 16+zlib.MAX_WBITS)
            # reader = csv.DictReader(StringIO.StringIO(body),
            #                         fieldnames=['email_address', 'name'])
            # for row in reader:
            #     recipients.append({row['email_address'].strip(), row['name'].strip()})

        # Read the message files
        try:
            response = s3.get_object(Bucket=args['bucket'], Key=args['plain_text_template'])
            mime_message_text = response['Body'].read().decode("utf-8")
            if args['template_variables']:
                t = Template(mime_message_text)
                mime_message_text = t.render(args['template_variables'])
            t = Template(mime_message_text)
            mime_message_text = t.render(something="World")
        except Exception as e:
            logger.info(e)
            mime_message_text = None
            logger.info('Failed to read text message file. Did you upload %s?' % args['plain_text_template'])
        try:
            response = s3.get_object(Bucket=args['bucket'], Key=args['html_template'])
            mime_message_html = response['Body'].read().decode("utf-8")
            if args['template_variables']:
                t = Template(mime_message_html)
                mime_message_html = t.render(args['template_variables'])
        except Exception as e:
            logger.info(e)
            mime_message_html = None
            logger.info('Failed to read html message file. Did you upload %s?' % args['html_template'])

        if not mime_message_text and not mime_message_html:
            raise ValueError('Cannot continue without a text or html message file.')

        # Send in parallel using several threads
        e = concurrent.futures.ThreadPoolExecutor(max_workers=max_threads)
        for recipient in recipients:
            from_address = "{}@{}".format(args['from_local_part'], from_domain)
            to_address = recipient['email_address']
            subject = event['Records'][0]['Sns']['Subject']
            t = Template(mime_message_html, variable_start_string='[[', variable_end_string=']]')
            mime_message_html = t.render(recipient_name=recipient['name'])
            message = mime_email(subject, from_address, to_address, mime_message_text, mime_message_html)
            e.submit(send_mail, from_address, to_address, message)
        e.shutdown()
    except Exception as e:
        logger.exception('Aborting... ' + str(e))
        raise e


if __name__ == "__main__":
    json_content = json.loads(open('event.json', 'r').read())
    lambda_handler(json_content, None)
