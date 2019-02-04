#!/bin/bash


HIVE="beeline -n hive -u 'jdbc:hive2://zk0-qq19.sbfsmq1f3etejmyvvrfr1sniia.cx.internal.cloudapp.net:2181,zk1-qq19.sbfsmq1f3etejmyvvrfr1sniia.cx.internal.cloudapp.net:2181,zk4-qq19.sbfsmq1f3etejmyvvrfr1sniia.cx.internal.cloudapp.net:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' "

# Create the text/flat tables as external tables. These will be later be converted to ORCFile.
echo "Loading text data into external tables."
runcommand "$HIVE  -i settings/load-flat.sql -f ddl-tpcds/text/alltables.sql --hivevar DB=tpcds_text_${SCALE} --hivevar LOCATION=${DIR}/${SCALE}"

# Create the partitioned and bucketed tables.
if [ "X$FORMAT" = "X" ]; then
	FORMAT=orc
fi

LOAD_FILE="load_${FORMAT}_${SCALE}.mk"
SILENCE="2> /dev/null 1> /dev/null" 
if [ "X$DEBUG_SCRIPT" != "X" ]; then
	SILENCE=""
fi

echo -e "all: ${DIMS} ${FACTS}" > $LOAD_FILE

i=1
total=24
DATABASE=tpcds_bin_partitioned_${FORMAT}_${SCALE}
MAX_REDUCERS=2500 # maximum number of useful reducers for any scale 
REDUCERS=$((test ${SCALE} -gt ${MAX_REDUCERS} && echo ${MAX_REDUCERS}) || echo ${SCALE})

# Populate the smaller tables.
for t in ${DIMS}
do
	COMMAND="$HIVE  -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/${t}.sql \
	    --hivevar DB=tpcds_bin_partitioned_${FORMAT}_${SCALE} --hivevar SOURCE=tpcds_text_${SCALE} \
            --hivevar SCALE=${SCALE} \
	    --hivevar REDUCERS=${REDUCERS} \
	    --hivevar FILE=${FORMAT}"
	echo -e "${t}:\n\t@$COMMAND $SILENCE && echo 'Optimizing table $t ($i/$total).'" >> $LOAD_FILE
	i=`expr $i + 1`
done

for t in ${FACTS}
do
	COMMAND="$HIVE  -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/${t}.sql \
	    --hivevar DB=tpcds_bin_partitioned_${FORMAT}_${SCALE} \
            --hivevar SCALE=${SCALE} \
	    --hivevar SOURCE=tpcds_text_${SCALE} --hivevar BUCKETS=${BUCKETS} \
	    --hivevar RETURN_BUCKETS=${RETURN_BUCKETS} --hivevar REDUCERS=${REDUCERS} --hivevar FILE=${FORMAT}"
	echo -e "${t}:\n\t@$COMMAND $SILENCE && echo 'Optimizing table $t ($i/$total).'" >> $LOAD_FILE
	i=`expr $i + 1`
done

make -j 1 -f $LOAD_FILE


echo "Loading constraints"
runcommand "$HIVE -f ddl-tpcds/bin_partitioned/add_constraints.sql --hivevar DB=tpcds_bin_partitioned_${FORMAT}_${SCALE}"

echo "Data loaded into database ${DATABASE}."
