# mit6824 MIT的分布式课程lab

#Lab 1: MapReduce

测试的文件位于`src/mapreduce/test_test.go `文件中, 写好map.reduce函数后，第一个测试按照下面的步骤执行
```
export "GOPATH=$PWD"
cd "$GOPATH/src/mapreduce"
go test -run Sequential
```
如果代码有bug，可以go test -run SequentialSingle 这个测试比较简单
