echo Test1
java -jar SlidingWindowReceiver-5785.jar -ip=127.0.0.1 -port=5000 -outfile=outfiles/Test1-file1-out.txt -rws=3 > Test1-receiver-output.txt & 
java -jar SlidingWindowSender-5785.jar -dest=127.0.0.1 -port=5000 -f=tests/file1.txt -sws=3 -packetsize=100 -rtt=100 > Test1-sender-output.txt

echo Test2
java -jar SlidingWindowReceiver-5785.jar -ip=127.0.0.1 -port=5001 -outfile=outfiles/Test2-file1-out.txt -rws=5 > Test2-receiver-output.txt & 
java -jar SlidingWindowSender-5785.jar -dest=127.0.0.1 -port=5001 -f=tests/file1.txt -sws=10 -packetsize=100 -rtt=150 > Test2-sender-output.txt

echo Test3
java -jar SlidingWindowReceiver-5785.jar -ip=127.0.0.1 -port=5002 -outfile=outfiles/Test3-file1-out.txt -rws=7 > Test3-receiver-output.txt & 
java -jar SlidingWindowSender-5785.jar -dest=127.0.0.1 -port=5002 -f=tests/file1.txt -sws=20 -packetsize=50 -rtt=80 > Test3-sender-output.txt

echo Test4
java -jar SlidingWindowReceiver-5785.jar -ip=127.0.0.1 -rws=10 -port=5003 -outfile=outfiles/Test4-file2-out.txt  > Test4-receiver-output.txt &
java -jar SlidingWindowSender-5785.jar -dest=127.0.0.1 -port=5003 -f=tests/file2.txt -sws=5 -packetsize=1000 -rtt=200 -droplist=10 > Test4-sender-output.txt

echo Test5
java -jar SlidingWindowReceiver-5785.jar -ip=127.0.0.1 -port=5004 -outfile=outfiles/Test5-file2-out.txt -rws=20 > Test5-receiver-output.txt & 
java -jar SlidingWindowSender-5785.jar -dest=127.0.0.1 -port=5004 -f=tests/file2.txt -sws=40 -packetsize=1020 -rtt=250 -droplist=10,550 > Test5-sender-output.txt

echo Test6
java -jar SlidingWindowReceiver-5785.jar -ip=127.0.0.1 -port=5005 -outfile=outfiles/Test6-file2-out.txt -rws=30 > Test6-receiver-output.txt & 
java -jar SlidingWindowSender-5785.jar -dest=127.0.0.1 -port=5005 -f=tests/file2.txt -sws=50 -packetsize=500 -rtt=11 -droplist=1,10,20,600 > Test6-sender-output.txt

echo Test7
java -jar SlidingWindowReceiver-5785.jar -ip=127.0.0.1 -port=5006 -outfile=outfiles/Test7-file3-out.pdf -rws=40 > Test7-receiver-output.txt & 
java -jar SlidingWindowSender-5785.jar -dest=127.0.0.1 -port=5006 -f=tests/file3.pdf -sws=40 -packetsize=600 -rtt=100 -droplist=5,100,58,200 > Test7-sender-output.txt

echo Test8
java -jar SlidingWindowReceiver-5785.jar -ip=127.0.0.1 -port=5007 -outfile=outfiles/Test8-file3-out.pdf -rws=50 > Test8-receiver-output.txt & 
java -jar SlidingWindowSender-5785.jar -dest=127.0.0.1 -port=5007 -f=tests/file3.pdf -sws=50 -packetsize=700 -rtt=150 -droplist=1,2,3,100,200,300,400,500 > Test8-sender-output.txt

echo Test9
java -jar SlidingWindowReceiver-5785.jar -ip=127.0.0.1 -port=5008 -outfile=outfiles/Test9-file3-out.pdf -rws=60 > Test9-receiver-output.txt & 
java -jar SlidingWindowSender-5785.jar -dest=127.0.0.1 -port=5008 -f=tests/file3.pdf -sws=100 -packetsize=30 -rtt=200 -droplist=2,3,52,800,801,802,900,1024,1025,3003,4004,5005,6006,7007,13000 > Test9-sender-output.txt

echo Test10
java -jar SlidingWindowReceiver-5785.jar -ip=127.0.0.1 -port=5009 -outfile=outfiles/Test10-file3-out.pdf -rws=70 > Test10-receiver-output.txt & 
java -jar SlidingWindowSender-5785.jar -dest=127.0.0.1 -port=5009 -f=tests/file3.pdf -sws=70 -packetsize=25 -rtt=150 -droplist=10,25,85,90,97,100,200,1000 > Test10-sender-output.txt

echo Test11
java -jar SlidingWindowReceiver-5785.jar -ip=127.0.0.1 -port=5010 -outfile=outfiles/Test11-file3-out.pdf -rws=80 > Test11-receiver-output.txt & 
java -jar SlidingWindowSender-5785.jar -dest=127.0.0.1 -port=5010 -f=tests/file3.pdf -sws=85 -packetsize=900 -rtt=300 -droplist=50,450,1000 > Test11-sender-output.txt

echo Test12
java -jar SlidingWindowReceiver-5785.jar -ip=127.0.0.1 -port=5011 -outfile=outfiles/Test12-file1-out.txt -rws=19 > Test12-receiver-output.txt & 
java -jar SlidingWindowSender-5785.jar -dest=127.0.0.1 -port=5011 -f=tests/file1.txt -sws=20 -packetsize=80 -rtt=400 -droplist=2,3,30,40 > Test12-sender-output.txt

echo Test13
java -jar SlidingWindowReceiver-5785.jar -ip=127.0.0.1 -port=5012 -outfile=outfiles/Test13-file2-out.txt -rws=100 > Test13-receiver-output.txt & 
java -jar SlidingWindowSender-5785.jar -dest=127.0.0.1 -port=5012 -f=tests/file2.txt -sws=120 -packetsize=95 -rtt=200 -droplist=5,15,25,9,71,501,2000,10000,10500 > Test13-sender-output.txt

echo Test14
java -jar SlidingWindowReceiver-5785.jar -ip=127.0.0.1 -port=5013 -outfile=outfiles/Test14-file1-out.txt -rws=110 > Test14-receiver-output.txt & 
java -jar SlidingWindowSender-5785.jar -dest=127.0.0.1 -port=5013 -f=tests/file1.txt -sws=10 -packetsize=122 -rtt=800 -droplist=7 > Test14-sender-output.txt

echo Test15
java -jar SlidingWindowReceiver-5785.jar -ip=127.0.0.1 -port=5014 -outfile=outfiles/Test15-file2-out.txt -rws=110 > Test15-receiver-output.txt & 
java -jar SlidingWindowSender-5785.jar -dest=127.0.0.1 -port=5014 -f=tests/file2.txt -sws=120 -packetsize=90 -rtt=10 -droplist=11,23,800,1200,4000,4002,5000,10000,10100,11000,11111 > Test15-sender-output.txt

echo Missing parameter 1
java -jar SlidingWindowReceiver-5785.jar -ip=127.0.0.1 -port=5014 -outfile=outfiles/Test16-file2-out.txt > Test16-receiver-output.txt
java -jar SlidingWindowSender-5785.jar -dest=127.0.0.1 -port=5014 -f=tests/file2.txt -sws=120 -packetsize=90 -droplist=11,23,800,1200,4000,4002,5000,10000,10100,11000,11111 > Test16-sender-output.txt

echo Missing parameter 2
java -jar SlidingWindowReceiver-5785.jar -ip=127.0.0.1 -port=5014 -rws=110> Test17-receiver-output.txt
java -jar SlidingWindowSender-5785.jar -dest=127.0.0.1 -port=5014 -f=tests/file2.txt -sws=120 -rtt=10 -droplist=11,23,800,1200,4000,4002,5000,10000,10100,11000,11111 > Test17-sender-output.txt

echo Illegal parameter 1
java -jar SlidingWindowReceiver-5785.jar -ip=127.0.0.1 -port=5014 -outfile=outfiles/Test18-file2-out.txt -rws=-5 > Test18-receiver-output.txt 
java -jar SlidingWindowSender-5785.jar -dest=127.0.0.1 -port=abc -f=tests/file2.txt -sws=120 -packetsize=90 -rtt=10 -droplist=11,23,800,1200,4000,4002,5000,10000,10100,11000,11111 > Test18-sender-output.txt

echo Illegal parameter 2
java -jar SlidingWindowReceiver-5785.jar -ip=300.0.1 -port=5013 -outfile=outfiles/Test19-file1-out.txt -rws=110 > Test19-receiver-output.txt 
java -jar SlidingWindowSender-5785.jar -dest=127.0.0.1 -port=5013 -f=tests/file1.txt -sws=-10 -packetsize=122 -rtt=800 -droplist=7 > Test19-sender-output.txt

echo Check output files
sha256sum -c sliding-window-results-sums.sha256 > outfiles/file-checksum-results.txt
