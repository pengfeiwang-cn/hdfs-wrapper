#include "luxor_hdfs_pipe_NamedPipe.h"
#include <jni.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>


JNIEXPORT jstring JNICALL Java_luxor_hdfs_pipe_NamedPipe_createPipeInternal(JNIEnv *env, jobject, jstring str) {
    const char *path = env->GetStringUTFChars(str, NULL);
    jstring ret = NULL;

    int res = mkfifo(path, 0666);
    if (res == -1) {
        ret = env->NewStringUTF(strerror(errno));
    }

    env->ReleaseStringUTFChars(str, path);

    return ret;
}