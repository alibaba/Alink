#!/usr/bin/env bash
#mapfile array <user.properties
array=()
while read line|| [[ -n ${line} ]]
do
    array+=("$line")
done<../../user.properties

len=${#array[*]}
PACKAGES=""
for i in $(seq 3 1 9)
do
    PACKAGES="$PACKAGES ${array[i]}"
done

pip_version=${array[1]}
option=${array[2]}
OLD_IFS="$IFS"
IFS="="
pip_array=($pip_version)
option_array=($option)
IFS="$OLD_IFS"
pipV=${pip_array[1]}
option=${option_array[1]}

${pipV} install ${option} ${PACKAGES}