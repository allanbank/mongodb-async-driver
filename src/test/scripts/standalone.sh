#! /bin/sh

# sharded.sh
#
# Provides the ability to start and stop a standalone configuration of 
# MongoDB mongod process.
#
# mongod ervers on ports 27017

tmpdir="/dev/shm"
dirname=standalone

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
	
	port=27017
	mongod --port ${port} --fork --dbpath "${dir}" \
				--logpath ${dir}/mongod.log \
				>> ${dir}/mongod.out 2>&1
	waitfor "${port}" "${dir}/mongod.log"
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
