
#ifndef __CFLX_USERCALL_LIB
#define __CFLX_USERCALL_LIB

#include <libsyscall_intercept_hook_point.h>
#include <syscall.h>
#include <errno.h>
#include "dcache_manager.c"


pid_t user_TID = 0;


int intercept_open(char* fname, int oflag, mode_t mode, int* result){

    int ret;
    int namelen = strlen(fname) + 1;
    if (namelen < 16) namelen = 16;
    if (!callnamebuf) callnamebuf = (char*)malloc(4096);
    char *namebuf = callnamebuf;
    memset(namebuf, 0, namelen);
    memcpy(namebuf, fname, namelen-1);

    if (memcmp("/confluxFS", namebuf, 10) == 0){ 
        char *qxdfs_fname = fname + 10;
        // if (user_TID == 0) user_TID = syscall(__NR_gettid);
        // fprintf(stderr, "$$$ Intercepted #%d open func: %s.$$$\n", user_TID, fname);
        int o_flag = 0;
        if (oflag & O_CREAT) {
            // fprintf(stderr, "$$$ Intercepted #%d open,creat func: %s.$$$\n", user_TID, fname);
            o_flag = 1;
        }
        struct openfile_ret* openret = qdfs_client_open(qxdfs_fname, o_flag);
        *result = openret->ord_fd;

        file_pointer[openret->ord_fd - COPY_QXDFS_FDBASE] = 0;
        fstat_size[openret->ord_fd - COPY_QXDFS_FDBASE] = openret->stat_size;

        
        // fprintf(stderr, "$$$ Intercepted #%d open result: %d.$$$\n", user_TID, openret->ord_fd);
        return 0;
    }
    else{
        
        return 1;
    }
}

int intercept_openat(int dirfd, char* fname, int oflag, mode_t mode, int* result){

    int ret;
    int namelen = strlen(fname) + 1;
    if (namelen < 16) namelen = 16;
    if (!callnamebuf) callnamebuf = (char*)malloc(4096);
    char *namebuf = callnamebuf;
    memset(namebuf, 0, namelen);
    memcpy(namebuf, fname, namelen-1);

    if (memcmp("/confluxFS", namebuf, 10) == 0){
        char *qxdfs_fname = fname + 10;
        // if (user_TID == 0) user_TID = syscall(__NR_gettid);
        // fprintf(stderr, "$$$ Intercepted #%d openat func: %s.$$$\n", user_TID, fname);
        int o_flag = 0;
        if (oflag & O_CREAT) {
            // fprintf(stderr, "$$$ Intercepted #%d open,creat func: %s.$$$\n", user_TID, fname);
            o_flag = 1;
        }
        struct openfile_ret* openret = qdfs_client_open(qxdfs_fname, o_flag);
        *result = openret->ord_fd;

        file_pointer[openret->ord_fd - COPY_QXDFS_FDBASE] = 0;
        fstat_size[openret->ord_fd - COPY_QXDFS_FDBASE] = openret->stat_size;

        
        // fprintf(stderr, "$$$ Intercepted #%d openat result: %d.$$$\n", user_TID, openret->ord_fd);
        return 0;
    }
    else{
        
        return 1;
    }
}

int intercept_close(int fd, int* result){
    int ret;
    if (fd >= COPY_QXDFS_FDBASE){
        // fprintf(stderr, "$$$ Intercepted close func: %d.$$$\n", fd);
        int iid = lcache->fd2iid_log[fd-COPY_QXDFS_FDBASE];
        fileis_dirty[iid] = 0;
        ret = qdfs_client_close(fd);
        file_pointer[fd-COPY_QXDFS_FDBASE] = -1;
        *result = ret;

        return 0;
    }
    else{
        return 1;
    }
}

int intercept_read(int fd, void* buf, size_t len, size_t* result){
    int ret;
    if (fd >= COPY_QXDFS_FDBASE){

        off_t fp = file_pointer[fd-COPY_QXDFS_FDBASE];
        // if (user_TID == 0) user_TID = syscall(__NR_gettid);
        // fprintf(stderr, "$$$ Intercepted #%d read func: %d, %ld, %ld.$$$\n", user_TID, fd, fp, len);
        uint64_t curlen = qdfs_file_getlen(fd);
        if (fp >= curlen) {
            *result = 0;
            return 0;
        }
        if (fp + len > curlen)
            len = curlen - fp;
        qdfs_client_pread(fd, buf, len, fp); 
        fp += len;
        file_pointer[fd-COPY_QXDFS_FDBASE] = fp;

        *result = len;

        return 0;
    }
    else{
        return 1;
    }
}

int intercept_write(int fd, void* buf, size_t len, size_t* result){
    int ret;
    if (fd >= COPY_QXDFS_FDBASE){

        off_t fp = file_pointer[fd-COPY_QXDFS_FDBASE];
        // if (user_TID == 0) user_TID = syscall(__NR_gettid);
        // fprintf(stderr, "$$$ Intercepted #%d write func: %d, %ld, %ld.$$$\n", user_TID, fd, fp, len);
        if (len <= 0){
            *result = 0;
            return 0;
        }
        qdfs_client_pwrite(fd, buf, len, fp);
        fp += len;
        file_pointer[fd-COPY_QXDFS_FDBASE] = fp;
        if (fp > fstat_size[fd-COPY_QXDFS_FDBASE])
            fstat_size[fd-COPY_QXDFS_FDBASE] = fp;
        int iid = lcache->fd2iid_log[fd-COPY_QXDFS_FDBASE];
        fileis_dirty[iid] = 1;

        *result = len;

        return 0;
    }
    else{
        return 1;
    }
}

int intercept_fadvise(int fd, off_t offset, off_t length, int adv, int* result){
    
    int ret;
    if (fd >= COPY_QXDFS_FDBASE){
        // fprintf(stderr, "$$$ Intercepted fadvise func: %d.$$$\n", fd);
        *result = 0;
        return 0;
    }
    else{
        return 1;
    }
}

int intercept_pread64(int fd, void* buf, size_t n, loff_t offset, size_t* result){
    size_t ret;
    if (fd >= COPY_QXDFS_FDBASE){
        // fprintf(stderr, "$$$ Intercepted pread func: %d.$$$\n", fd);
        uint64_t curlen = qdfs_file_getlen(fd);
        if (curlen == 0) {
            *result = 0;
            return 0;
        }
        if (offset + n > curlen)
            n = curlen - offset;
        qdfs_client_pread(fd, buf, n, offset);
        *result = n;

        return 0;
    }
    else{
        return 1;
    }
}

int intercept_pwrite64(int fd, void* buf, size_t n, loff_t offset, size_t* result){
    size_t ret;
    if (fd >= COPY_QXDFS_FDBASE){
        // fprintf(stderr, "$$$ Intercepted pwrite func: %d.$$$\n", fd);
        ssize_t ret = qdfs_client_pwrite(fd, buf, n, offset);
        *result = ret;

        int iid = lcache->fd2iid_log[fd-COPY_QXDFS_FDBASE];
        fileis_dirty[iid] = 1;

        return 0;
    }
    else{
        return 1;
    }
}

int intercept_lseek(int fd, off_t offset, int whence, int* result){
    int ret;
    if (fd >= COPY_QXDFS_FDBASE){
        // fprintf(stderr, "$$$ Intercepted lseek func: %d.$$$\n", fd);
        
        if (whence == SEEK_SET)
            file_pointer[fd-COPY_QXDFS_FDBASE] = offset;
        else if (whence == SEEK_CUR) 
            file_pointer[fd-COPY_QXDFS_FDBASE] += offset;
        else if (whence == SEEK_END){
            uint64_t curlen = qdfs_file_getlen(fd);
            file_pointer[fd-COPY_QXDFS_FDBASE] = (off_t)curlen + offset;
        }
        else 
            fprintf(stderr, "[WARN]: Conflux has not implemented lseek with whence %d.\n", whence);
        *result = 0;

        return 0;
    }
    else{
        return 1;
    }
}

int intercept_creat(char* fname, mode_t mode, int* result){
    
    int ret;
    int namelen = strlen(fname) + 1;
    if (namelen < 16) namelen = 16;
    if (!callnamebuf) callnamebuf = (char*)malloc(4096);
    char *namebuf = callnamebuf;
    memset(namebuf, 0, namelen);
    memcpy(namebuf, fname, namelen-1);

    if (memcmp("/confluxFS", namebuf, 10) == 0){
        char *qxdfs_fname = fname + 10;

        ret = qdfs_client_creat(qxdfs_fname, mode);
        file_pointer[ret - COPY_QXDFS_FDBASE] = 0;
        fstat_size[ret - COPY_QXDFS_FDBASE] = 0;

        *result = ret;
        
        return 0;
    }
    else {
        
        return 1;
    }
}

int intercept_mkdir(const char* dirname, mode_t mode, int* result){
    
    int ret;
    int namelen = strlen((char*)dirname) + 1;
    if (namelen < 16) namelen = 16;
    if (!callnamebuf) callnamebuf = (char*)malloc(4096);
    char *namebuf = callnamebuf;
    memset(namebuf, 0, namelen);
    memcpy(namebuf, dirname, namelen-1);

    if (memcmp("/confluxFS", namebuf, 10) == 0){
        // fprintf(stderr, "$$$ Intercepted mkdir func: %s.$$$\n", dirname);
        char *qxdfs_dname = (char*)dirname + 10;

        ret = qdfs_client_mkdir(qxdfs_dname, mode);
        
        *result = 0;
        
        return 0;
    }
    else{
        
        return 1;
    }
}

int intercept_fsync(int fd, int* result){
    
    int ret;
    if (fd >= COPY_QXDFS_FDBASE){
        // fprintf(stderr, "$$$ Intercepted fsync func: %d.$$$\n", fd);
        qdfs_client_fsync(fd);
        int iid = lcache->fd2iid_log[fd-COPY_QXDFS_FDBASE];
        fileis_dirty[iid] = 0;
        
        *result = 0;
        return 0;
    }
    else{
        return 1;
    }
}

int intercept_truncate(const char* fname, off_t len, int* result){
    
    int ret;
    int namelen = strlen(fname) + 1;
    if (namelen < 16) namelen = 16;
    if (!callnamebuf) callnamebuf = (char*)malloc(4096);
    char *namebuf = callnamebuf;
    memset(namebuf, 0, namelen);
    memcpy(namebuf, fname, namelen-1);

    if (memcmp("/confluxFS", namebuf, 10) == 0){
        // fprintf(stderr, "$$$ Intercepted truncate func: %s.$$$\n", fname);
        char *qxdfs_fname = (char*)fname + 10;
        struct openfile_ret* openret = qdfs_client_open(qxdfs_fname, 0);
        int openedfd = openret->ord_fd;
        qdfs_file_setlen(openedfd, len);
        qdfs_client_close(openedfd);

        *result = 0;
        
        return 0;
    }
    else{
        
        return 1;
    }
}


int intercept_ftruncate(int fd, off_t len, int* result){

    int ret;
    if (fd >= COPY_QXDFS_FDBASE){
        // fprintf(stderr, "$$$ Intercepted ftruncate func: %d.$$$\n", fd);
        ret = qdfs_file_setlen(fd, len);
        fstat_size[fd-COPY_QXDFS_FDBASE] = (uint64_t)len;

        *result = ret;
        return 0;
    }
    else{
        return 1;
    }
}

int intercept_fallocate(int fd, int mode, off_t offset, off_t len, int* result){
    
    int ret;
    if (fd >= COPY_QXDFS_FDBASE){
        // fprintf(stderr, "$$$ Intercepted fallocate func: %d.$$$\n", fd);
        uint64_t curlen = qdfs_file_getlen(fd);
        if (curlen < offset+len){
            ret = qdfs_file_setlen(fd, offset+len);
            fstat_size[fd-COPY_QXDFS_FDBASE] = (uint64_t)len;
        }

        *result = ret;
        return 0;
    }
    else{
        return 1;
    }
}

int intercept_stat(const char* fname, struct stat* stat_cont, int* result){
    
    int ret;
    int namelen = strlen(fname) + 1;
    if (namelen < 16) namelen = 16;
    if (!callnamebuf) callnamebuf = (char*)malloc(4096);
    char *namebuf = callnamebuf;
    memset(namebuf, 0, namelen);
    memcpy(namebuf, fname, namelen-1);

    if (memcmp("/confluxFS", namebuf, 10) == 0){
        // fprintf(stderr, "$$$ Intercepted stat func: %s.$$$\n", fname);
        char *qxdfs_fname = (char*)fname + 10;

        struct openfile_ret* statret = qdfs_client_stat(qxdfs_fname, 2);

        int statfd = statret->ord_fd;
        int statmode = statret->file_mode;
        uint64_t statsize = statret->stat_size;

        if (statfd < 0) {
            // fprintf(stderr, "$$$ Intercepted stat func: %s, NON-EXIST.$$$\n", fname);
            *result = -1;
            
            return 0;
        }
        if (statmode == 0){
            stat_cont->st_dev = 1;
            stat_cont->st_ino = statfd;
            stat_cont->st_mode = __S_IFREG;
            stat_cont->st_size = (off_t)statsize;
            stat_cont->st_nlink = 1;
            *result = 0;
        }
        else {
            stat_cont->st_dev = 1;
            stat_cont->st_ino = statfd;
            stat_cont->st_mode = __S_IFDIR;
            stat_cont->st_size = 4096;
            stat_cont->st_nlink = 1;
            *result = 0;
        }
        
        return 0;
    }
    else {
        
        return 1;
    }
}

int intercept_lstat(const char* fname, struct stat* stat_cont, int* result){
    
    int ret;
    int namelen = strlen(fname) + 1;
    if (namelen < 16) namelen = 16;
    if (!callnamebuf) callnamebuf = (char*)malloc(4096);
    char *namebuf = callnamebuf;
    memset(namebuf, 0, namelen);
    memcpy(namebuf, fname, namelen-1);

    if (memcmp("/confluxFS", namebuf, 10) == 0){
        // fprintf(stderr, "$$$ Intercepted lstat func: %s.$$$\n", fname);
        char *qxdfs_fname = (char*)fname + 10;

        struct openfile_ret* statret = qdfs_client_stat(qxdfs_fname, 2);

        int statfd = statret->ord_fd;
        int statmode = statret->file_mode;
        uint64_t statsize = statret->stat_size;

        if (statfd < 0) {
            *result = -1;
            
            return 0;
        }
        if (statmode == 0){
            stat_cont->st_size = (off_t)statsize;
            stat_cont->st_nlink = 1;
            *result = 0;
        }
        else {
            stat_cont->st_size = 0;
            stat_cont->st_nlink = 1;
            *result = 0;
        }
        
        return 0;
    }
    else {
        
        return 1;
    }
}

int intercept_fstat(int fd, struct stat* stat_cont, int* result){
    
    int ret;
    if (fd >= COPY_QXDFS_FDBASE){
        // fprintf(stderr, "$$$ Intercepted fstat func: %d.$$$\n", fd);
        uint64_t curlen = qdfs_file_getlen(fd);
        stat_cont->st_size = (off_t)curlen;
        stat_cont->st_nlink = 1;

        *result = 0;
        return 0;
    }
    else {
        return 1;
    }
}

int intercept_unlink(char* fname, int* result){
    
    int ret;
    int namelen = strlen(fname) + 1;
    if (namelen < 16) namelen = 16;
    if (!callnamebuf) callnamebuf = (char*)malloc(4096);
    char *namebuf = callnamebuf;
    memset(namebuf, 0, namelen);
    memcpy(namebuf, fname, namelen-1);

    if (memcmp("/confluxFS", namebuf, 10) == 0){
        // fprintf(stderr, "$$$ Intercepted unlink func: %s.$$$\n", fname);
        int rmv_ret = qdfs_client_remove(fname);

        *result = rmv_ret;
        return 0;
    }
    else {
        
        return 1;
    }
}

int intercept_unlinkat(int dirfd, char* fname, int* result){
    
    int ret;
    int namelen = strlen(fname) + 1;
    if (namelen < 16) namelen = 16;
    if (!callnamebuf) callnamebuf = (char*)malloc(4096);
    char *namebuf = callnamebuf;
    memset(namebuf, 0, namelen);
    memcpy(namebuf, fname, namelen-1);

    if (memcmp("/confluxFS", namebuf, 10) == 0){
        // fprintf(stderr, "$$$ Intercepted unlinkat func: %s.$$$\n", fname);
        int rmv_ret = qdfs_client_remove(fname);

        *result = rmv_ret;
        return 0;
    }
    else {
        
        return 1;
    }
}


static int
hook(long syscall_number,
        long arg0, long arg1,
        long arg2, long arg3,
        long arg4, long arg5,
        long *result)
{
    switch (syscall_number) {
        case SYS_open: return intercept_open((char*)arg0, (int)arg1, (mode_t)arg2, (int*)result);
        case SYS_openat: return intercept_openat((int)arg0, (char*)arg1, (int)arg2, (mode_t)arg3, (int*)result);
        case SYS_close: return intercept_close((int)arg0, (int*)result);
        case SYS_read: return intercept_read((int)arg0, (void*)arg1, (size_t)arg2, (size_t*)result);
        case SYS_write: return intercept_write((int)arg0, (void*)arg1, (size_t)arg2, (size_t*)result);
        case SYS_fadvise64: return intercept_fadvise((int)arg0, (off_t)arg1, (off_t)arg2, (int)arg3, (int*)result);
        case SYS_pread64: return intercept_pread64((int)arg0, (void*)arg1, (size_t)arg2, (loff_t)arg3, (size_t*)result);
        case SYS_pwrite64: return intercept_pwrite64((int)arg0, (void*)arg1, (size_t)arg2, (loff_t)arg3, (size_t*)result);
        case SYS_lseek: return intercept_lseek((int)arg0, (off_t)arg1, (int)arg2, (int*)result);
        case SYS_creat: return intercept_creat((char*)arg0, (mode_t)arg1, (int*)result);
        case SYS_mkdir: return intercept_mkdir((const char*)arg0, (mode_t)arg1, (int*)result);
        case SYS_fsync: return intercept_fsync((int)arg0, (int*)result);
        case SYS_truncate: return intercept_truncate((const char*)arg0, (off_t)arg1, (int*)result);
        case SYS_ftruncate: return intercept_ftruncate((int)arg0, (off_t)arg1, (int*)result);
        case SYS_fallocate: return intercept_fallocate((int)arg0, (int)arg1, (off_t)arg2, (off_t)arg3, (int*)result);
        case SYS_stat: return intercept_stat((const char*)arg0, (struct stat*)arg1, (int*)result);
        case SYS_lstat: return intercept_lstat((const char*)arg0, (struct stat*)arg1, (int*)result);
        case SYS_fstat: return intercept_fstat((int)arg0, (struct stat*)arg1, (int*)result);
        case SYS_unlink: return intercept_unlink((char*)arg0, (int*)result);
        case SYS_unlinkat: return intercept_unlinkat((int)arg0, (char*)arg1, (int*)result);
    }
    return 1;
}

static __attribute__((constructor)) void
init(void)
{
	// Set up the callback function
	intercept_hook_point = hook;
}

#endif
