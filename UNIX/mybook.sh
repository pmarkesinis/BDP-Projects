#!/usr/bin/env bash
# ---- MY AMAZING BOOK ----
cd ./../data/myBook/ || exit

# -- Q1 --
echo "-- Q1 --"
# Write a pipeline that prints the 10 most common words in all text files for the current directory.
# Your pipeline should be case insensitive and ignore punctuation.
# Example output:
# 14 book
# 10 cover
mostCommonWords=$(cat *.txt | tr -d '[:punct:]' | tr '[:upper:]' '[:lower:]'| xargs -n1 | sort | uniq -c | sort -nr | head -10)
# Prints the mostCommonWords
echo "Most common words in my book:"
echo "$mostCommonWords"


echo "--------"


# -- Q2 --
echo "-- Q2 --"
# Write a pipeline that places each sentence of the book on a new line.
# Make sure that they are in the same order as they appear in the book (i.e., first the sentences from the intro, followed by the sentences from chapter1 etc.).
# Store only the first 7 sentences.
# You don't have to remove leading or trailing spaces. However, we do encourage you to try.
# Example output:
#
# Far far away, behind the word mountains, far from the countries Vokalia and Consonantia, there live the blind texts
# Separated they live in Bookmarksgrove right at the coast of the Semantics, a large language ocean
linesFromTheBook=$(cat *.txt | tr -s '.' '\n' | head -7)
echo "Listing of lines from the book:"
echo "$linesFromTheBook"


echo "--------"


# -- Q3 --
echo "-- Q3 --"
# It seems that the writer of the book mistyped the word "I" and used a lower case "i" instead.
# Write a pipeline that finds all the text files and replaces all the words "i" with its uppercase variant.
# Make sure that it is NOT inline and that the output book is in its original order.
fixedBook=$(cat *.txt | sed -r 's/ i / I /g' | sed -r 's/"i /"I /g')
echo "The corrected book:"
echo "$fixedBook"
echo "--------"



# End on start path.
cd ../../pipelines/ || exit