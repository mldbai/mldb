#!/bin/bash

if [ $# -lt 2 ] ;
    then
    echo "usage: " $0 "<regex> <replacement> file...";
    exit;
fi

REGEX=$1
REPLACEMENT=$2
shift 2

FILES=$@

#echo "files = " $FILES

for file in $FILES ;
  do
  echo $file
  cp -f $file $file~
  cat $file~ | sed "s!$REGEX!$REPLACEMENT!g" > $file~~ && mv $file~~ $file
  chmod --reference=$file~ $file
done
