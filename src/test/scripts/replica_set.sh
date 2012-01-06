#! /bin/sh

# replica_set.sh
#
# Provides the ability to start and stop a replica set configuration of 
# Mongodb processes.
#
# Shard servers on ports 27018, 27019, 27020, and 27021
# Arbriter on port 27017


tmpdir="${TMPDIR:-/tmp}"
dirname=replica_set

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

	mkdir "${dir}/replica1"
	mkdir "${dir}/replica2"
	mkdir "${dir}/replica3"
	mkdir "${dir}/replica4"
	mkdir "${dir}/arbiter"
	
	# A single arbiter.
	server=arbiter
	port=27017
	mongod --port ${port} --fork --dbpath "${dir}/${server}" \
				--smallfiles --logpath ${dir}/${server}.log \
				--replSet ${dirname} \
				>> ${dir}/${server}.out 2>&1
	waitfor "${port}" "${dir}/${server}.log"

	# 4 Mongod replicas servers.
	server=replica1
	port=27018
	mongod --port ${port} --fork --dbpath "${dir}/${server}" \
				--smallfiles --logpath ${dir}/${server}.log \
				--replSet ${dirname} \
				>> ${dir}/${server}.out 2>&1
	waitfor "${port}" "${dir}/${server}.log"
	
	server=replica2
	port=27019
	mongod --port ${port} --fork --dbpath "${dir}/${server}" \
				--smallfiles --logpath ${dir}/${server}.log \
				--replSet ${dirname} \
				>> ${dir}/${server}.out 2>&1
	waitfor "${port}" "${dir}/${server}.log"
	
	server=replica3
	port=27020
	mongod --port ${port} --fork --dbpath "${dir}/${server}" \
				--smallfiles --logpath ${dir}/${server}.log \
				--replSet ${dirname} \
				>> ${dir}/${server}.out 2>&1
	waitfor "${port}" "${dir}/${server}.log"
						
	server=replica4
	port=27021
	mongod --port ${port} --fork --dbpath "${dir}/${server}" \
				--smallfiles --logpath ${dir}/${server}.log \
				--replSet ${dirname} \
				>> ${dir}/${server}.out 2>&1
	waitfor "${port}" "${dir}/${server}.log"
						
	# Initiate the replica set.
	cat <<END >> ${dir}/config.js
	rs.initiate({
		_id: "${dirname}",
		members: [
			{
				_id: 0,
				host:"localhost:27017",
				arbiterOnly:true
			},
			{
				_id: 1,
				host:"localhost:27018"
			},
			{
				_id: 2,
				host:"localhost:27019"
			},
			{
				_id: 3,
				host:"localhost:27020"
			},
			{
				_id: 4,
				host:"localhost:27021"
			}
		]
	})
END
	mongo localhost:27018/admin ${dir}/config.js	
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
