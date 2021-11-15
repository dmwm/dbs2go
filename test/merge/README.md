This area contains benchmarks for various ways to insert large number
of records (on the order of 1M) into ORACLE DB. We used the following
approaches:
- use `INSERT ALL` procedure with private temporary table
- use specific chunk size to address ORACLE connection/cursor limitation
- use max size along with chunk size where the former specify total
number of insert records to be inserted via concurrent insert of chunks

Below you can find all benchmark results for a simple case of using
table with two integers and merging it to main table, see `main.go`
code for more details.

To compile the code just adjust `oci8.pc` file to reflect your ORACLE
installation and build code using `go build` command.

### INSERT ALL procedure with temp table
We start with basic benchmark of inserting 1K records
```
./dbtest -dbfile dbfile -nrec 1000
2021/11/15 19:58:08 execute INSERT ALL 1000
2021/11/15 19:58:09 elapsed time for inserting 1000 records into temp table 1.10742141s
2021/11/15 19:58:09 elapsed time for merge step 1.141302418s
2021/11/15 19:58:09 metrics RSS 4.3384832e+07
2021/11/15 19:58:09 metrics RSS increase 2.5714688e+07 (25.7MB)
2021/11/15 19:58:09 elapsed time 1.151854493s
```

Now, we can see how much time we need for 10K records
```
./dbtest -dbfile dbfile -nrec 10000
2021/11/15 19:58:35 execute INSERT ALL 10000
2021/11/15 20:00:10 elapsed time for inserting 10000 records into temp table 1m41.133455279s
2021/11/15 20:00:10 elapsed time for merge step 1m41.190663277s
2021/11/15 20:00:10 metrics RSS 2.45977088e+08
2021/11/15 20:00:10 metrics RSS increase 2.28532224e+08 (228.5MB)
2021/11/15 20:00:10 elapsed time 1m41.220692807s
```
And, finally we can reach ORACLE limit on number of bind parameters:
```
./dbtest -dbfile dbfile -nrec 100000
2021/11/15 20:00:45 elapsed time for creation of temp table 12.466944ms
2021/11/15 20:00:45 metrics RSS 1.767424e+07
2021/11/15 20:05:17 execute INSERT ALL 100000
2021/11/15 20:22:02 unable to insert all into temp tableORA-16951: Too many bind variables supplied for this SQL statement.
```
Conclusion: we can't have more than 100K binding parameters, but 10K is still ok

### using chunk size 1K along with temp table
With that in mind we introduce chunk size and check the results
```
./dbtest -dbfile dbfile -nrec 10000
2021/11/15 20:32:09 elapsed time for inserting 10000 records into temp table 1.959244165s
2021/11/15 20:32:09 elapsed time for merge step 2.076558051s
2021/11/15 20:32:09 metrics RSS 1.3633536e+08
2021/11/15 20:32:09 metrics RSS increase 1.18628352e+08 (118.6MB)
2021/11/15 20:32:09 elapsed time 2.106842573s
```
```
# chunk size 1k
./dbtest -dbfile dbfile -nrec 100000
2021/11/15 20:34:38 elapsed time for inserting 100000 records into temp table 13.001495422s
2021/11/15 20:34:38 elapsed time for merge step 13.234055199s
2021/11/15 20:34:38 metrics RSS 4.02808832e+08
2021/11/15 20:34:38 metrics RSS increase 3.85318912e+08 (385.3MB)
2021/11/15 20:34:38 elapsed time 13.300458539s
```

```
# chunk size 5K
./dbtest -dbfile dbfile -nrec 100000 -chunk 5000
2021/11/15 20:36:33 elapsed time for inserting 100000 records into temp table 52.617635755s
2021/11/15 20:36:33 elapsed time for merge step 52.763224607s
2021/11/15 20:36:34 metrics RSS 1.027702784e+09
2021/11/15 20:36:34 metrics RSS increase 1.01013504e+09 (1.0GB)
2021/11/15 20:36:34 elapsed time 54.563068005s
```

```
# chunk size 1K
./dbtest -dbfile dbfile -nrec 100000 -chunk 1000
2021/11/15 20:42:35 elapsed time for creation of temp table 2.704193ms
2021/11/15 20:42:35 metrics RSS 1.769472e+07
2021/11/15 20:42:48 elapsed time for inserting 100000 records into temp table 12.915921826s
2021/11/15 20:42:48 elapsed time for merge step 13.184547575s
2021/11/15 20:42:48 metrics RSS 5.70281984e+08
2021/11/15 20:42:48 metrics RSS increase 5.52587264e+08 (552.6MB)
2021/11/15 20:42:48 elapsed time 13.272630248s
```

It seems that larger chunk size is not always good. We see that with 1K
chunk size we can achive 4 times better results than with 5K one.
But, again we hit the limit on ORACLE size on number of open cursors

```
./dbtest -dbfile dbfile -nrec 1000000 -chunk 1000
2021/11/15 20:43:39 elapsed time for creation of temp table 8.753359ms
2021/11/15 20:43:39 metrics RSS 1.7514496e+07
2021/11/15 20:45:04 unable to insert all into temp tableORA-01000: maximum open cursors exceeded
```

Conclusion: we can't have more than 1000 cursors, but 100 cursors are still fine

### using max size 100K, chunk size 1K
Finally, we modified code to include max size and use concurrently chunk
insertion
```
./dbtest -dbfile dbfile -nrec 1000000 -chunk 1000
2021/11/15 21:27:29 main.go:89: elapsed time for creation of temp table 2.386181ms
2021/11/15 21:27:29 main.go:93: metrics RSS 1.7391616e+07
2021/11/15 21:27:29 main.go:119: process 100 goroutines, step 0-100000, elapsed time 223.981µs
2021/11/15 21:27:38 main.go:119: process 100 goroutines, step 100000-200000, elapsed time 122.964µs
2021/11/15 21:27:46 main.go:119: process 100 goroutines, step 200000-300000, elapsed time 72.54µs
2021/11/15 21:27:55 main.go:119: process 100 goroutines, step 300000-400000, elapsed time 75.58µs
2021/11/15 21:28:03 main.go:119: process 100 goroutines, step 400000-500000, elapsed time 75.303µs
2021/11/15 21:28:12 main.go:119: process 100 goroutines, step 500000-600000, elapsed time 81.146µs
2021/11/15 21:28:21 main.go:119: process 100 goroutines, step 600000-700000, elapsed time 76.3µs
2021/11/15 21:28:30 main.go:119: process 100 goroutines, step 700000-800000, elapsed time 75.442µs
2021/11/15 21:28:39 main.go:119: process 100 goroutines, step 800000-900000, elapsed time 71.096µs
2021/11/15 21:28:48 main.go:119: process 100 goroutines, step 900000-1000000, elapsed time 81.736µs
2021/11/15 21:28:57 main.go:122: elapsed time for inserting 1000000 records into temp table 1m28.135184736s
2021/11/15 21:28:58 main.go:138: elapsed time for merge step 1m29.254958647s
2021/11/15 21:28:58 main.go:146: metrics RSS 4.92834816e+08
2021/11/15 21:28:58 main.go:147: metrics RSS increase 4.754432e+08 (475.4MB)
2021/11/15 21:28:58 main.go:148: elapsed time 1m29.481946274s
```
As you can see each concurrent insert ranges in O(100)µs range and we can
isnert 1M records in 90 seconds. Let's see how it will scale at 4M records

```
./dbtest -dbfile dbfile -nrec 4000000 -chunk 1000
2021/11/15 21:32:26 main.go:89: elapsed time for creation of temp table 1.861075ms
2021/11/15 21:32:26 main.go:93: metrics RSS 1.7457152e+07
2021/11/15 21:32:26 main.go:119: process 100 goroutines, step 0-100000, elapsed time 480.845µs
2021/11/15 21:32:35 main.go:119: process 100 goroutines, step 100000-200000, elapsed time 130.772µs
...
2021/11/15 21:38:34 main.go:122: elapsed time for inserting 4000000 records into temp table 6m8.28077052s
2021/11/15 21:38:39 main.go:138: elapsed time for merge step 6m13.382883565s
2021/11/15 21:38:40 main.go:146: metrics RSS 6.27204096e+08
2021/11/15 21:38:40 main.go:147: metrics RSS increase 6.09746944e+08 (609.7MB)
2021/11/15 21:38:40 main.go:148: elapsed time 6m13.921475676s
```
So, we can insert 4M records can be in about 400sec on 10 core node using around 600MB.
Increasing number of cores to 16 does not gain much. And, we achieve
similar performace by using different max size:
```
./dbtest -dbfile dbfile -nrec 4000000 -chunk 1000 -maxSize 200000
2021/11/15 21:52:35 main.go:91: elapsed time for creation of temp table 2.470205ms
2021/11/15 21:52:35 main.go:95: metrics RSS 2.306048e+07
2021/11/15 21:58:54 main.go:139: elapsed time for merge step 6m19.468122996s
2021/11/15 21:58:55 main.go:147: metrics RSS 7.3895936e+08
2021/11/15 21:58:55 main.go:148: metrics RSS increase 7.1589888e+08 (715.9MB)
2021/11/15 21:58:55 main.go:149: elapsed time 6m20.072620512s
```

But, if we decrease chunk size (i.e. use more goroutines) we can gain almost a
minute (this round uses 400 goroutines)
```
./dbtest -dbfile dbfile -nrec 4000000 -chunk 500 -maxSize 200000
2021/11/15 23:09:03 main.go:91: elapsed time for creation of temp table 2.19021ms
2021/11/15 23:09:03 main.go:95: metrics RSS 2.9298688e+07
2021/11/15 23:14:31 main.go:123: elapsed time for inserting 4000000 records into temp table 5m27.575693657s
2021/11/15 23:14:39 main.go:139: elapsed time for merge step 5m35.335210313s
2021/11/15 23:14:39 main.go:147: metrics RSS 7.75491584e+08
2021/11/15 23:14:39 main.go:148: metrics RSS increase 7.46192896e+08 (746.2MB)
2021/11/15 23:14:39 main.go:149: elapsed time 5m35.905232556s
```

If we'll increase number of goroutines to 800 we hit again ORACLE threshold:
```
./dbtest -dbfile dbfile -nrec 4000000 -chunk 250 -maxSize 200000
2021/11/15 23:48:10 main.go:91: elapsed time for creation of temp table 1.921319ms
2021/11/15 23:48:10 main.go:95: metrics RSS 2.3109632e+07
2021/11/15 23:48:10 main.go:120: process 800 goroutines, step 0-200000, elapsed time 5.6199ms
2021/11/15 23:48:22 main.go:172: unable to insert all into temp tableORA-01000: maximum open cursors exceeded
```

Finally, using 1K chunk size with 300K max size we achieve the following:
```
./dbtest -dbfile dbfile -nrec 4000000 -chunk 1000 -maxSize 300000
2021/11/16 00:06:13 main.go:91: elapsed time for creation of temp table 1.916505ms
2021/11/16 00:06:13 main.go:95: metrics RSS 2.5153536e+07
2021/11/16 00:12:28 main.go:123: elapsed time for inserting 4000000 records into temp table 6m15.038055358s
2021/11/16 00:12:35 main.go:139: elapsed time for merge step 6m22.431921257s
2021/11/16 00:12:36 main.go:147: metrics RSS 9.88516352e+08
2021/11/16 00:12:36 main.go:148: metrics RSS increase 9.63362816e+08 (963.4MB)
2021/11/16 00:12:36 main.go:149: elapsed time 6m22.993527923s
```

So, it seems that `-chunk 500 -maxSize 200000` is the best performance options
which allows to insert 4M in 5 and half minutes using 750MB of RAM. And, these
set of parameters leads to 1m15s and 575MB for 1M records insertion.
