#!/bin/sh

BATCH_SIZE=10
BEGIN_DATE='2017-01-01'
END_DATE='2017-03-10'

# "Symbol","Name","LastSale","MarketCap","IPOyear","Sector","industry","Summary Quote"
NASDAQ_URL='http://www.nasdaq.com/screening/companies-by-name.aspx?exchange=NASDAQ&render=download'
AMEX_URL='http://www.nasdaq.com/screening/companies-by-name.aspx?exchange=AMEX&render=download'
NYSE_URL='http://www.nasdaq.com/screening/companies-by-name.aspx?exchange=NYSE&render=download'

hdfs_companies="/stocks/companies"
hdfs_history="/stocks/history"
hdfs_current="/stocks/current"

function get_from_yahoo() {
  curl -s `echo "$1" | sed 's/ /%20/g'` > $2 
}

function load() {
  tmp_file="/tmp/stocks/$1_company_list"
  symbol_file="/tmp/stocks/$1_symbols"

  echo "--- BEGIN loading $1 ---"

  echo "- Load the company list"
  curl -s $2 | tail -n +2 > $tmp_file

  echo "- Put company list into HDFS"
  hdfs dfs -put $tmp_file $hdfs_companies

  num=`wc -l $tmp_file | cut -d' ' -f1`

  # clean up symbols  
  cat $tmp_file | cut -d',' -f1 | tr -d "\n" | sed 's/ //g'| sed 's/\"\"/\",\"/g' > $symbol_file

  echo "- Get historical data"
  missing="/tmp/stocks/$1_missing_historical_symbols"
  for (( i=0; i<=num; i+=$BATCH_SIZE )); do
    range="$((i+1))-$((i+BATCH_SIZE))"
    echo "-- loading $range of $num"
    symbols=`cat $symbol_file | cut -d',' -f$range`
    historical_url="http://query.yahooapis.com/v1/public/yql?q=select * from yahoo.finance.historicaldata where symbol in ($symbols) and startDate=\"$BEGIN_DATE\" and endDate=\"$END_DATE\"&format=json&env=store://datatables.org/alltableswithkeys"
    data_file="/tmp/stocks/$1_historical_$range"
    curl -s `echo "$historical_url" | sed 's/ /%20/g'` > $data_file
    for s in $(echo $symbols | sed 's/,/ /g'); do
       if [[ $(grep -c $s $data_file) -eq 0 ]]; then
         curl -s `echo "$historical_url" | sed 's/ /%20/g'` > $data_file
         if [[ $(grep -c $s $data_file) -eq 0 ]]; then
           printf "$s," >> $missing
         fi
       fi
    done

  done

  if [[ -e $missing ]]; then
    echo "- Symbols not loaded: `cat $missing`"
  fi

  #echo "- Load historical data into HDFS"
  #hdfs dfs -put /tmp/stocks/$1_historical* $hdfs_history

  echo "--- DONE loading $1 ---"
}

# clean up
rm -rf /tmp/stocks
mkdir /tmp/stocks/
hdfs dfs -rm "$hdfs_companies/*"
hdfs dfs -rm "$hdfs_history/*"
#hdfs dfs -rm "$hdfs_history/*"
#hdfs dfs -rm "$hdfs_current/*"

load "nasdaq" "$NASDAQ_URL"
#load "amex" "$AMEX_URL"
#load "nyse" "$NYSE_URL"

rm -rf /tmp/hdfs
