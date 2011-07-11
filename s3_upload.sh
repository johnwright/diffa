#!/bin/sh

BUCKET=s3://diffa-packages

s3cmd rb --recursive $BUCKET
s3cmd mb $BUCKET
s3cmd put dist/target/*.zip $BUCKET