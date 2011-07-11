#!/bin/sh

BUCKET=s3://diffa-packages

sed s/DIFFA_BUILD_NUMBER/$1/ etc/packages.js.in > packages.js

s3cmd rb --recursive $BUCKET

s3cmd mb $BUCKET

s3cmd put dist/target/*.zip $BUCKET
s3cmd put agent/target/*.war $BUCKET
s3cmd put packages.js $BUCKET