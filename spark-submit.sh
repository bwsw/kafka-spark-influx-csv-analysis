#!/bin/bash

# Copyright 2017, bwsoft management
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

export PYTHONHASHSEED=0
APP_ARGS=$*
echo "args = $APP_ARGS"

TMP_DIR=/tmp
APP_ARGS=""
REPACK=false

while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    --master)
    MASTER="$2"
    shift
    ;;
    --dependencies)
    DEPS_DIR="$2"
    shift
    ;;
    --repack)
    REPACK="$2"
    shift
    ;;
    --help)
    echo "example: ./spark-submit.sh --master spark://192.168.1.1:7077 --dependencies tmp main.py $(pwd)/config.json"
    echo "set --repack true if you want repack existing packages"
    exit
    ;;
    *)
    APP_ARGS+=$key" "       # unknown option
    ;;
esac
shift 
done

if [ -z ${DEPS_DIR} ]
then
    DEPS_DIR="deps-build"
fi

BASE_DIR=$(pwd)

if echo $DEPS_DIR | grep -v -P '^/'
then
    DEPS_DIR=$BASE_DIR/$DEPS_DIR
fi

mkdir -p $DEPS_DIR

if [ ! -d $DEPS_DIR ];
then
    echo "Failed to create $DEPS_DIR. Exit"
    exit 1
fi

if [[ ! -d 'dist' ]] || [[ $REPACK == 'true' ]]
then
    python setup.py bdist_egg
    name_egg_app=$(cat setup.py | grep name | sed -e 's/\(^.*\"\)\(.*\)\(\".*$\)/\2/') #'
    echo "name_egg_app=$name_egg_app"
    egg_name=$(find dist -name "$name_egg_app*" -print0 | xargs -0 ls)
    echo "egg_name=$egg_name"
    cp $egg_name $DEPS_DIR
fi

DEPS_LIST=()

cd $TMP_DIR

while read description url
do
    file_name=$(echo ${url} | sed -e 's/\(^.*\/\)\(.*\)/\2/') #'
    DEPS_LIST+=($file_name)
    if [ -f $TMP_DIR/$file_name ]; then
        echo "File '$TMP_DIR/$file_name' exists, skip downloading."
    else
        wget -O $TMP_DIR/$file_name ${url}
    fi
done < $BASE_DIR/submit-dependencies

for file in "${DEPS_LIST[@]}"
do
    if [[ $file == *"zip"* ]]; then
        unzip -oq $file
        base_name=${file%.zip}
    else
        tar -xzf $file
        base_name=${file%.tar.gz}
    fi
    if [[ ! -d $base_name"/dist" ]] || [[ $REPACK == 'true' ]]
    then
        cd $base_name
        echo "base_name=$base_name"
        python setup.py bdist_egg
        egg_name=$(find dist -name "$base_name*" -print0 | xargs -0 ls)
        echo "egg_name=$egg_name"
        cp $egg_name $DEPS_DIR
        cd ..
    fi
done

cd $BASE_DIR
echo "Application Arguments: '$APP_ARGS'"

DEPS=""
for f in $(ls $DEPS_DIR)
do
        DEPS="$DEPS,$DEPS_DIR/$f"
done

DEPS=${DEPS:1}


CONFIGS=""
for f in $(ls conf*json)
do
    CONFIGS="$CONFIGS,$f"
done

DEPS=$(sed 's/,//' <<< "${DEPS},${CONFIGS:1}")

CMD="spark-submit --deploy-mode client --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 --py-files $DEPS"

if [ -n ${MASTER} ]
then
    CMD="$CMD --master $MASTER"
fi

CMD="$CMD $APP_ARGS"

echo "Running $CMD"
$CMD
