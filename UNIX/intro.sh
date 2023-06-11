#!/usr/bin/env bash

# This is a bash script file.
# To run this file use `./intro.sh` in your terminal while in the correct directory.
# If you get "Permission denied" error, execute `chmod +x intro.sh` (This makes the file executable).

# The `echo` command prints lines to the output. If run using `./intro.sh`, then the output will be on the terminal.
echo "Running intro.sh"

# The `cd` command (change directory) can be used to change the current directory.
# Update the `cd` command below to make sure that the rest of this script is executed in the apacheLog folder, that is inside of the data folder:
cd ../data/apacheLog || exit
# The '||' on this line represents the logical `or` operator. If the left part gives an exit code of non-zero (error/false), then the right part is evaluated.
# If the left part gives an exit code of zero (successful), then the right part is not evaluated at all.
# We also have the '&&' operator (and). This only evaluates the right part if the left part has a zero exit code (no error/true).
echo "Moved from intro to the data->apacheLog folder"

# The following line will execute the `ls` command and save it in the variable lsOutput:
lsOutput=$(ls)
# Then we can use `echo` to print the result:
echo "Result of ls:"
echo "$lsOutput"

# Implement a command  that counts the number files  in the current directory.
nmrOfFiles=$(ls | wc -l)
echo "Number of files:"
echo "$nmrOfFiles"

# Implement a command that displays all files and their details in the current directory.
# Do this by typing it between the brackets below.
# You can first test individual commands by pasting them in the terminal.
# (Make sure that you are in the correct directory if you want to test it)
detailedLsOutput=$(ls -l)
# Prints the detailedLsOutput
echo "Files in directory:"
echo "$detailedLsOutput"

# Implement a command that looks for all files containing '_log' in their name
logFiles=$(find . -name '*_log*')
# Print result
echo "Log files:"
echo "$logFiles"

# Implement a command that gets all the lines that contain "File does not exist" from the 'error_log' file, which can be found in the `data` directory:
lines=$(grep "File does not exist" 'error_log')
# Print result
echo "Lines from error_log where 'File does not exist:'"
echo "$lines"

# Now that you have used the basics of finding files and getting text from files, it's time to move on.
# The real power of these commands is in the ability to combine them.
# Using the output of the first command as the input for the second is called *piping* and is denoted by the symbol `|`
# A silly example would be: `ls | lolcat`
echo "The ls command but now with some more color"
ls | lolcat

# Let's create a pipeline where we get the first 3 GET requests that were requested from the access_log file, which can be found under the `data` directory.
# The output of a line should look like: "GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1"
# The first part should take all the lines containing "GET" from the 'access_log' file.
# The second part should only keep the first 3 lines.
# Lastly, we need to get the data that is in between the quotes.
firstPipeline=$(grep 'GET' access_log | head -3)
echo "First pipeline results:"
echo "$firstPipeline"

# Implement a pipeline that displays only the owner of the log files in the current directory.
secondPipeline=$(ls -l | grep '_log' | awk '{print $3}')
# Print result
echo "Second pipeline results:"
echo "$secondPipeline"

# End on start path.
# This is needed for the automated testing.
cd ../../intro || exit
