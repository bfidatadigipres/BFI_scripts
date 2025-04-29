#!/usr/bin/python3.8

'''
Small script to check in terminal contents of bucket
Run calling python3 alone will launch boto3 resource request
and show all files present in folder

2021
'''

import os
import boto3

# Global variables
BUCKET = os.environ.get('DALET_BUCKET')

s3 = boto3.resource('s3')
my_bucket = s3.Bucket(BUCKET)

for bucket_object in my_bucket.objects.all():
    print(bucket_object)
