#!/usr/bin/env bash

# Create different versions of the .NET for Apache Spark runtime docker image
# based on the Apach Spark and .NET for Apache Spark version.

set -o errexit # abort on nonzero exit status
set -o nounset # abort on unbound variable
set -o pipefail # dont hide errors within pipes

readonly dotnet_core_version=3.1
readonly dotnet_spark_version=2.1.1
readonly apache_spark_version=3.2.1
readonly apache_spark_short_version="${apache_spark_version:0:3}"
readonly hadoop_short_version=2.7
readonly scala_version=2.11
proxy=""

main() {
    echo "Building .NET for Apache Spark ${dotnet_spark_version} runtime image with Apache Spark ${apache_spark_version}"

    build_dotnet_sdk
    build_dotnet_spark_base_runtime
    build_dotnet_spark_runtime

    trap finish EXIT ERR

    exit 0
}

#######################################
# Runs the docker build command with the related build arguments
# Arguments:
#   The image name (incl. tag)
# Result:
#   A local docker image with the specified name
#######################################
build_image() {
    local image_name="${1}"
    local docker_file_name="${2}"
    local build_args="--build-arg DOTNET_CORE_VERSION=${dotnet_core_version}
        --build-arg DOTNET_SPARK_VERSION=${dotnet_spark_version}
        --build-arg SPARK_VERSION=${apache_spark_version}
        --build-arg HADOOP_VERSION=${hadoop_short_version}"
    local cmd="docker build ${build_args} -f ${docker_file_name} -t ${image_name} ."

    if [ -n "${proxy}" ]
    then
        build_args+=" --build-arg HTTP_PROXY=${proxy} --build-arg HTTPS_PROXY=${proxy}"
    fi

    echo "Building ${image_name}"

    ${cmd}
}

#######################################
# Use the Dockerfile in the sub-folder dotnet-sdk to build the image of the first stage
# Result:
#   A dotnet-sdk docker image tagged with the .NET core version
#######################################
build_dotnet_sdk() {
    local image_name="dotnet-sdk:${dotnet_core_version}"
    local docker_file_name="Dockerfile.dotnet-sdk"

    build_image "${image_name}" "${docker_file_name}"
}

#######################################
# Use the Dockerfile in the sub-folder dotnet-spark-base to build the image of the second stage
# The image contains the specified .NET for Apache Spark version plus the HelloSpark example
#   for the correct TargetFramework and Microsoft.Spark package version
# Result:
#   A dotnet-spark-base-runtime docker image tagged with the .NET for Apache Spark version
#######################################
build_dotnet_spark_base_runtime() {
    local image_name="dotnet-spark-base-runtime:${dotnet_spark_version}"
    local docker_file_name="Dockerfile.dotnet-spark-base"

    build_image "${image_name}" "${docker_file_name}"
}

#######################################
# Use the Dockerfile in the sub-folder dotnet-spark to build the image of the last stage
# The image contains the specified Apache Spark version
# Result:
#   A dotnet-spark docker image tagged with the .NET for Apache Spark version and the Apache Spark version.
#######################################
build_dotnet_spark_runtime() {
    local image_name="dotnet-spark:${dotnet_spark_version}-${apache_spark_version}"
    local docker_file_name="Dockerfile.dotnet-spark"

    build_image "${image_name}" "${docker_file_name}"
}


finish()
{
    result=$?
    exit ${result}
}

main "${@}"