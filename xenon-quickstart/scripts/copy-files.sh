#!/bin/bash
#
# Copy archetype resources from directory where they can be built and tested
# to directory where Maven experts to find archetype templates. 
# In the process, replace 'example.group' with ${groupId}
#
SOURCES="QuickstartHost.java EmployeeService.java"
TEST_SOURCES="TestUtils.java EmployeeServiceTest.java"

TEMPLATE_ROOT=src/main/resources/archetype-resources

SDIR=src/main/java/example/group
TEMPLATE_SDIR=src/main/java

TDIR=src/test/java/example/group
TEMPLATE_TDIR=src/test/java

TS=`date "+%m%d%H%M%Y.%S"`   # timestamp

#
# $1 -> source directory
# $2 -> target directory
# $3 -> list of files to be copied.
# also accumulates paths to be added to the archetype.xml
function copyFiles {
    sourceDir=$1 && shift
    targetDir=$1 && shift
    files=$*
    for file in ${files}; do
        source=${sourceDir}/${file}
        target=${TEMPLATE_ROOT}/${targetDir}/${file}
        mkdir -p ${TEMPLATE_ROOT}/${targetDir}
        sed 's/example.group/${groupId}/g' ${source} > ${target}
    done
}

###############
# Main program
###############

copyFiles ${SDIR} ${TEMPLATE_SDIR} ${SOURCES}
copyFiles ${TDIR} ${TEMPLATE_TDIR} ${TEST_SOURCES}

# Now construct the manifest file

for file in ${SOURCES} ; do
    xml_f="${TEMPLATE_SDIR}/${file}"
    xml_f_list="${xml_f_list}<source>${xml_f}</source>"$'\n'
done
for file in ${TEST_SOURCES} ; do
    xml_f="${TEMPLATE_TDIR}/${file}"
    xml_t_f_list="${xml_t_f_list}<source>${xml_f}</source>"$'\n'
done
