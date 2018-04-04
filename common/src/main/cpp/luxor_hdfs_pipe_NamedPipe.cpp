#include "luxor_hdfs_pipe_NamedPipe.h"
#include <jni.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>
//#include <iostream>
//#include <fstream>
//
//using namespace std;

JNIEXPORT jstring JNICALL Java_luxor_hdfs_pipe_NamedPipe_createPipeInternal(JNIEnv *env, jclass, jstring str) {
    const char *path = env->GetStringUTFChars(str, NULL);
    jstring ret = NULL;

//ofstream f;
//f.open("/tmp/fucku");
//cout << "Java_luxor_hdfs_pipe_NamedPipe_createPipeInternal:'" << path << "'." << endl;

    int res = mkfifo(path, 0666);
//cout << "res=" << res << endl;
    if (res == -1) {
        ret = env->NewStringUTF(strerror(errno));
//        cout << "err:" << strerror(errno) << endl;
    }
    env->ReleaseStringUTFChars(str, path);
    return ret;
}