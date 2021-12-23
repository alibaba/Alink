#!/usr/bin/env bash

SCRIPT_PATH=$(dirname "$0")

if [[ "${SCRIPT_PATH:0:1}" = '/' ]]; then
    echo "USE RAWPATH, SCRIPT_PATH: ${SCRIPT_PATH}"
else
    SCRIPT_PATH=$(cd $(dirname "$0"); pwd)
    echo "USE PWD, SCRIPT_PATH: ${SCRIPT_PATH}"
fi



# export PATH="${SCRIPT_PATH}/bin:$PATH"
# echo "SCRIPT_PATH: ${SCRIPT_PATH}"

# # unset PYTHONHOME if set
# if ! [ -z "${PYTHONHOME+_}" ] ; then
#     _OLD_VIRTUAL_PYTHONHOME="$PYTHONHOME"
#     echo "OLD PYTHONHOME: ${PYTHONHOME}"
#     unset PYTHONHOME
# fi

# if [ ! -z "${PYTHONPATH}" ] ; then
#     echo "OLD_PYTHONPATH: ${PYTHONPATH}"
#     unset PYTHONPATH
# fi

# if [ ! -z "${LD_LIBRARY_PATH}" ]; then
#     echo "OLD LD_LIBRARY_PATH: ${LD_LIBRARY_PATH}"
#     unset LD_LIBRARY_PATH
# fi


if [ ! -z "${LD_PRELOAD}" ]; then
    echo "OLD LD_PRELOAD: ${LD_PRELOAD}"
    unset LD_PRELOAD
fi


# export PYTHONHOME="${SCRIPT_PATH}"

if [ -n "${BASH-}" ] || [ -n "${ZSH_VERSION-}" ] ; then
    hash -r 2>/dev/null
fi

# exec python -c "from alink.py4j_gateway import main;main()" "$@"
exec python "${SCRIPT_PATH}/alink/py4j_gateway.py" "$@"
# exec ${SCRIPT_PATH}/bin/python -m alink.py4j_main "$@"
