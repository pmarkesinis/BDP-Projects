#!/usr/bin/env bash
echo "Running logParsing.sh"

# ---- APACHE ----
cd ./../data/apacheLog || exit

# -- Q1 --
echo "-- Q1 --"
# Write a pipeline that gets all POST requests from the `access_log` file and displays time, clientIP, path, statusCode and size. These values should be comma-separated.
# Example output: 10/Mar/2004:12:02:59,10.0.0.153,/cgi-bin/mailgraph2.cgi,200,2987
accessData=$(grep 'POST' access_log | awk -F'[ ]' '{print $4 "," $1 "," $7 "," $9 "," $10}' | cut -c 2-)
# cut -d " " -f 4 
# sed -e 's/$/,/'
# awk -F '[ -]'  '$2"-"$1'
# Print the accessData
echo "Access data:"
echo "$accessData"


echo "--------"


# -- Q2 --
echo "-- Q2 --"
# Write a pipeline that returns the IP address and the path of the largest-sized response to a POST request.
# Example output: 192.168.0.1,/actions/logout
# Hint: you could re-use the `accessData` variable to make it easier.
#largestResponse=$( sort -nk 2 < $accessData | tail -1 )
largestResponse=$(echo "$accessData" | awk -F'[,]' '{print $2 "," $3 "," $5}' | sort -t, -k3,3 -nr | head -1 |  awk -F'[,]' '{print $1 "," $2}')

echo "The largest Response was to:"
echo "$largestResponse"


echo "--------"


# -- Q3--
echo "-- Q3 --"
# Write a pipeline that returns the amount and the IP address of the client that sent the most POST requests.
# Example output: 20 192.168.0.1
# Hint: you could re-use the `accessData` variable to make it easier.
mostRequests=$(echo "$accessData" | awk -F'[,]' '{print $2}' | sort | uniq -c | sort -nr | head -1)
echo "The most requests where done by:"
echo "$mostRequests"

echo "--------"

# End on start path.
cd ../../pipelines/ || exit
