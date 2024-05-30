go build ../server/
go build ../client/
go build ../cmd/

ip=`hostname -I | xargs`
echo $ip

if [[ $ip =~ (10\.10\.1\.[0-9]*) ]]; then
    ip=${BASH_REMATCH[1]}
    config=`cat ./config.json`
    server_regex="(\"([0-9\.]*)\":\s*\"tcp://${ip}[^\"]*\").*$"
    server_PID_file="server.pid"


    server_PID=$(cat "${server_PID_file}");

    if [ -z "${server_PID}" ]; then
        echo "Starting servers"
        while [[ "$config" =~ $server_regex ]]; do
            id=${BASH_REMATCH[2]}
            echo $id
            ./server -log_dir=. -log_level=debug -id $id -algorithm=iss &
            echo $! >> ${server_PID_file}
            config=${config/${BASH_REMATCH[1]}}
        done
    else
        echo "Servers are already started in this folder."
    fi

    config=`cat ./config.json`
    client_regex="(\"([0-9\.]*)\":\s*\"tcp://${ip}[^\"]*\").*$"
    client_PID_file="client.pid"


    client_PID=$(cat "${client_PID_file}");

    if [ -z "${client_PID}" ]; then
        echo "Starting clients"
        while [[ "$config" =~ $client_regex ]]; do
            id=${BASH_REMATCH[2]}
            echo $id
            ./client -id $id -algorithm iss -config config.json &
            echo $! >> ${client_PID_file}
            config=${config/${BASH_REMATCH[1]}}
        done
    else
        echo "Clients are already started in this folder."
    fi

    echo "Finished starting"
    sleep 150
    echo "Closing everything"
    ./stop.sh
else
    echo "IP not found"
fi