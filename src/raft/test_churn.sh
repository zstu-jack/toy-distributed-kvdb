DoTest(){
    go test -timeout 30s -run TestReliableChurn2C >> test_churn
    ret=$(cat out | grep 'apply error' | wc -l)
    if [ ${ret} -gt 0 ]; then
        exit 0
    fi
    echo "PASS"
}

DoTest
DoTest
DoTest
DoTest
DoTest
DoTest
DoTest
DoTest
DoTest
DoTest

DoTest
DoTest
DoTest
DoTest
DoTest
DoTest
DoTest
DoTest
DoTest
DoTest

DoTest
DoTest
DoTest
DoTest
DoTest
DoTest
DoTest
DoTest
DoTest
DoTest

DoTest
DoTest
DoTest
DoTest
DoTest
DoTest
DoTest
DoTest
DoTest
DoTest

DoTest
DoTest
DoTest
DoTest
DoTest
DoTest
DoTest
DoTest
DoTest
DoTest