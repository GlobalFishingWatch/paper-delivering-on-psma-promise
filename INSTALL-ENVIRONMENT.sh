#!/usr/bin/env bash

set -euo pipefail

# check pre-requisates programs
check_installed(){
    failmsg=${2:-"please install [${1}] with: brew install $1"}
    which $1 > /dev/null|| (echo ${failmsg}; exit 1)
}

os(){
    local machine
    unameOut="$(uname -s)"
    case "${unameOut}" in
	Linux*)     machine=Linux;;
	Darwin*)    machine=Mac;;
	CYGWIN*)    machine=Cygwin;;
	MINGW*)     machine=MinGw;;
	*)          machine="UNKNOWN:${unameOut}"
    esac
    echo ${machine}
}

_date() {
    if [ "$(os)" = "Mac" ]; then
	gdate "$@";
    else
	date "$@";
    fi
}


# check file dependencies
check_file_exists(){
    if [ ! -f ${1} ]; then
        echo "file [${BASE_ENV_FILE}] must exist";
        exit 1;
    fi
}

# functions supporting datetime operations on conda environments and files
get_conda_env(){
    conda env list --json | jq --raw-output --arg env_name "/$1" '.envs[] | select(endswith($env_name))'
}

# dates getters
get_conda_date(){
    local env_prefix
    env_prefix=$(get_conda_env ${1})
    # echo ${env_prefix}
    if [ -z "${env_prefix}" ]; then
	echo 0; # return zero seconds for datetime if the environment does not exist
    else
	local env_prefix env_hist
	env_hist=${env_prefix}/conda-meta/history
	# echo ${env_hist}
	env_date=$(head -n1 $env_hist | awk '{print $2 " " $3}')
	# echo ${env_date}
	env_secs=$(_date -d"${env_date}" "+%s")
	# echo ${env_secs}
	echo ${env_secs}
    fi
}

get_file_date(){
    _date -r ${1} "+%s"
}

# compare weather environment is older than file by environment name and file path
env_older_than_file(){
    local env_file env_name
    env_file=${1}
    env_name=${2:-$(cat ${env_file} | yq  --raw-output '.name')}

    local ENV_FILE_DATE ENV_DATE
    ENV_FILE_DATE=$(get_file_date ${env_file})
    ENV_DATE=$(get_conda_date ${env_name})

    if [ $ENV_FILE_DATE -ge $ENV_DATE ]; then
        echo "environment [${env_name}] older than describing file [${env_file}]"
        return 0
    else
        echo "environment [${env_name}] is ready"
        return 1
    fi
}

# environment builder
build_derived_local_environment(){
    local base_env work_env
    base_env=$1
    work_env=$2
    conda env remove -n ${work_env}

    # clone `radenv` so that it doesn't get corrupted by current project
    conda create --name ${work_env} --clone ${base_env}
    # install local `setup.cfg` in an editable way in new environment
    $(get_conda_env ${work_env})/bin/python -m pip install --editable .[all]

    $(get_conda_env ${work_env})/bin/python -m pre_commit install  # install pre-commit
}

check_installed conda "please install [conda] or [miniconda] as described on the website"
check_installed yq "please install [yq] through [conda], the version in [brew] is out of date"
check_installed jq
if [ "$(os)" = "Mac" ]; then
    check_installed gdate
fi

BASE_ENV_FILE=${1:-radenv.yaml}
check_file_exists ${BASE_ENV_FILE}
check_file_exists setup.py
check_file_exists setup.cfg

# create base environment if out of date
BASE_ENV=$(cat ${BASE_ENV_FILE} | yq  --raw-output '.name')
if env_older_than_file ${BASE_ENV_FILE} ${BASE_ENV}; then
    echo "rebuilding this environment may be unnescisary if you have an up to date [${BASE_ENV}] from another repository with the same dependencies"
    read -r -p "would your like to rebuild base environment from [${BASE_ENV_FILE}]? [y/N] " response
    case "$response" in
        [yY][eE][sS]|[yY])
            conda env remove -n ${BASE_ENV}
            conda env create -f ${BASE_ENV_FILE}
            INVALIDATE_CHILDREN=1
            ;;
        *)
            ;;
    esac
fi


WORK_ENV=$(python setup.py --name)
if [ -n "${INVALIDATE_CHILDREN:-}" ]; then
    build_derived_local_environment $BASE_ENV $WORK_ENV
else
    if env_older_than_file setup.cfg ${WORK_ENV}; then
        read -r -p "would your like to rebuild working environment named [${WORK_ENV}] from [setup.cfg]? [y/N] " response
        case "$response" in
            [yY][eE][sS]|[yY])
                build_derived_local_environment $BASE_ENV $WORK_ENV
                ;;
            *)
                ;;
        esac
    fi
fi

echo "to activate: conda activate ${WORK_ENV}"
