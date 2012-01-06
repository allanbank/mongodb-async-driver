#! /bin/sh

# sharded.sh
#
# Provides the ability to start and stop a sharded configuration of 
# Mongodb processes.
#
# Shard servers on ports 27018, 27020, and 27021
# Configuration servers on port 27019
# Mongos on port 27017

tmpdir="${TMPDIR:-/tmp}"
dirname=sharded

# stop
#
# Stops the shard server processes.
function stop {
	# Find all of the process based on the temp directory.
		if pkill -f "${tmpdir}/${dirname}-.*" >> /dev/null 2>&1  ; then
		let count=1
		while pkill -0 -f "${tmpdir}/${dirname}-.*"  >> /dev/null 2>&1  ; do
			sleep 1
			
			if (( count > 10 )) ; then
				pkill -9 -f "${tmpdir}/${dirname}-.*"  >> /dev/null 2>&1
			fi
			let count=count+1
		done
	fi
	# Cleanup the directories left behind.
	for file in $( find "${tmpdir}" -maxdepth 1 -name "${dirname}-*" ) ; do
		rm -rf "${file}"
	done
}

# waitfor
#
# Waits for a socket to open.
function waitfor {
	port=$1
	log=$2
	
	let count=1
	touch "${log}"
	while ! grep -q -i "waiting for connections on port ${port}" "${log}" ; do
		sleep 1
		
		if (( count > 10 )) ; then
			return;
		fi
		let count=count+1
	done
}

# start
#
# Starts the shard servers.
function start {
	# Make sure there are no process left over.
	stop
	
	dir=$( mktemp --directory -p "${tmpdir}" "${dirname}-XXXXXXXX" )

	mkdir "${dir}/shard1"
	mkdir "${dir}/shard2"
	mkdir "${dir}/shard3"
	mkdir "${dir}/mongos"
	mkdir "${dir}/config"
	
	# A Single config server.
	server=config
	port=27019
	mongod --configsvr --port ${port} --fork --dbpath "${dir}/${server}" \
				--smallfiles --logpath ${dir}/${server}.log \
				>> ${dir}/${server}.out 2>&1
	waitfor "${port}" "${dir}/${server}.log"
	
	# A single mongos.
	server=mongos
	port=27017
	mongos --port ${port} --fork \
				--logpath ${dir}/${server}.log \
				--configdb localhost:27019 \
				>> ${dir}/${server}.out 2>&1
	waitfor "${port}" "${dir}/${server}.log"

	# 3 Mongod shard servers.
	server=shard1
	port=27018
	mongod --shardsvr --port ${port} --fork --dbpath "${dir}/${server}" \
				--smallfiles --logpath ${dir}/${server}.log \
				>> ${dir}/${server}.out 2>&1
	waitfor "${port}" "${dir}/${server}.log"
	
	server=shard2
	port=27020
	mongod --shardsvr --port ${port} --fork --dbpath "${dir}/${server}" \
				--smallfiles --logpath ${dir}/${server}.log \
				>> ${dir}/${server}.out 2>&1
	waitfor "${port}" "${dir}/${server}.log"
	
	server=shard3
	port=27021
	mongod --shardsvr --port ${port} --fork --dbpath "${dir}/${server}" \
				--smallfiles --logpath ${dir}/${server}.log \
				>> ${dir}/${server}.out 2>&1
	waitfor "${port}" "${dir}/${server}.log"
						
	# Add the shards
	mongo localhost:27017/admin -eval "db.runCommand( { addshard : \"localhost:27018\" } );"
	mongo localhost:27017/admin -eval "db.runCommand( { addshard : \"localhost:27020\" } );"
	mongo localhost:27017/admin -eval "db.runCommand( { addshard : \"localhost:27021\" } );"
}

case "$1" in 
	start) 
		start
		;;
	stop ) 
		stop
		;;
	*)
		echo "Usage $0 {start|stop}"
		;;
esac
