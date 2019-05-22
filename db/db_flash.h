#include <stdint.h>
#include <string>

#include <sys/types.h>
#include <sys/mman.h>
#include <err.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <fcntl.h>    //provides O_RDONLY, 
#include <linux/fs.h>   //provides BLKGETSIZE
#include <sys/ioctl.h>  //provides ioctl()

#include <stdbool.h>

#include <vector>
#include <list>
#include <set>

#define OPEN_ARG

#define PAGE_BYTES 1024  //1KB
#define BLOCK_PAGES 8192 //4096 //4K pages a block
#define BLOCK_BYTES 4194304 //16777216//4194304// 8388608 //4194304 //4MB
#define SEGMENT_BLOCKS 1//#define AREA_BLOCKS 1 //1 block
#define SEGMENT_BYTES (SEGMENT_BLOCKS*BLOCK_BYTES)
#define VERSION_BYTES 4
#define MAGIC "MMAPKV"
#define MAGIC_BYTES 6

#define FILE_NAME_BYTES 24 //file name
#define FILE_ADDR_BYTES 8 //the segment offset
#define FILE_BYTES 8 //content bytes
#define ENTRY_BYTES 4096//(FILE_NAME_BYTES+FILE_ADDR_BYTES+..)
//segment
#define EXTENDED_ENTRY_NUM_BYTE_OFFSET 22
#define NON_LDB_ENTRY_NUM 32
#define DEV_NAME_LENGTH 9 //  /dev/sdp/

#define MMAP_PAGE 4096

#define FLUSH_BYTES 4096
namespace leveldb {

class Flash_file;

uint64_t AllocSeg(uint64_t next_file_number_);//根据文件名分配段
std::string get_file_name(const std::string& db_name);//获取文件名字

uint64_t entry_hash(const std::string& dbname);//获取hash值

class Flash{
	
		public:
		#define FLASH_OPEN_ARG  const std::string& dbname
		virtual int flash_open(FLASH_OPEN_ARG);//根据传入的设备名字打开flash
		virtual int flash_format(const std::string& dbname);
        //格式化flash
		virtual int flash_init(const std::string& dbname);
		//初始化flash
		
		int set_valid(char* addr, uint64_t offset, unsigned char value);
		//设置相应的值有效
		unsigned char * frequency_info;
        //频率信息
		unsigned char * validation_bitmap;
        //bitmap信息
		unsigned char * level0_entries;
        //level0的数目
		unsigned char * info;
		char * files_name_block;
		char *dev;
        //设备名字
		//char *dev_r;
		
		std::string db_dev;
		
		std::set<std::string> f_filenames;
        //文件集合
		std::vector<int> f_handles;

		std::string db_name;
		
		int dev_fd;//global
		int dev_fd_version;//for manifest
		int dev_fd_log;
		int dev_fd_sst;
		//FILE * dev_file;
		
		//df_flash_env
		int rename(const std::string&src, const std::string&target) ; //重命名
		int  remove(const std::string& filename);//移除文件
		virtual int access(const std::string& fname, int flag);
		int create_entry(const std::string& fname,uint64_t entry_offset, uint64_t segment_offset);
        //创造一个entry
		int delete_entry(const std::string& fname, int mode);
		//删除一个entry
        //virtual size_t read(void * ptr, size_t size, size_t count, char * addr );
		#define OPEN_FILE_ARG const std::string& fname, const char * mode
		Flash_file*  open_file(OPEN_FILE_ARG);//打开一个文件 

		int x();
		uint64_t segment_total;//总段数
		uint64_t ldb_entry_total;//
		
		//db_flash_tools.cc
		
		uint64_t get_entry_offset_by_name(const std::string& short_name);//根据文件名字获取entry的偏移量
		uint64_t get_seg_offset_by_name(const std::string& short_name);//根据文件名获取segment的偏移量
		
		
		uint64_t size;
		Flash(){
			printf("________-I am db_flash.h,  Flash construction\n");
		}
		void get(){//获取设备的大小
			printf("Hello, I'm set, size=%d\n",this->size);
		}
	
};

class Flash_file{//文件对象
	public:
	char * addr;// the file segment addr pointer
    //文件段的地址
	char *entry_addr;//the entry addr pointer
    //对应的文件描述符号的地址
	uint64_t offset_bytes;//the content  offset. This is the position the current operation should begin.
	//当前相对于文件段的偏移
	uint64_t content_bytes;// total content bytes. This is the total bytes the current file has stored.
	//文件当前存储的字节数目
	std::string file_name;//for test
	//文件名字
	//char *flush_addr;//for msync
	uint64_t flush_offset;//for msync
	//flush的位置
	uint64_t sync_offset;//for 
	//同步的偏移量
	uint64_t seg_offset;
    //段偏移
	int dev_fd;
    //设备的句柄
	void *buffer;
    //对应的缓冲区
	//char *w_buffer;
	
	Flash_file(){//构造函数 
		offset_bytes= ENTRY_BYTES;
		flush_offset= ENTRY_BYTES;
		sync_offset=ENTRY_BYTES;
		
		//this->buffer=(char *)malloc(SEGMENT_BYTES);	
		//512B对齐分配内存
        posix_memalign(&(this->buffer),512,SEGMENT_BYTES);	
		//初始化为0
        memset(this->buffer,0,ENTRY_BYTES);
		
	}
	~Flash_file(){
		//printf("I am ~Flash_file\n");
		free(buffer);
		
	}
	
};

}
