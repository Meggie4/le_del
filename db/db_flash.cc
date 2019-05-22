#include <stdlib.h>
#include <stdio.h>

#include "db/db_flash.h"

extern char * SegTable;// declare in db_flash_tools.cc
extern uint64_t SegTotal;// declare in db_flash_tools.cc

namespace leveldb{
extern int  is_flash_inited;



int Flash::flash_init(const std::string& dbname){
	//根据设备名初始化flash	
		if(is_flash_inited){//如果已经初始化了
			//printf("____________________________HHHHH, has inited___________\n");
			return 2; //2 says the flash has been inited
		}
		is_flash_inited=1;
		int fd=-1;
		int is_format=0;//是否格式化了
		fd=open(dbname.c_str(),O_RDWR);//打开设备
		
		if(fd<0){//如果fd < 0
			fprintf(stderr,"db_flash.cc, open err,exit,11111\n");
			exit(9);
		}
		
	    //设备句柄	
		this->dev_fd=fd;	//global fd	
		

		
		printf("init, fd=%d%d\n",fd);
		
		uint64_t blklb,blkpb,blk64, blk;
		blklb=blkpb=blk64=blk=0;
		
		if(dbname.find("/dev/")!=-1){
			ioctl(fd, BLKSSZGET, &blkpb);// is for physical sector size
			ioctl(fd, BLKBSZGET, &blklb);// is for logical sector size
			ioctl(fd, BLKGETSIZE64, &blk64);//reture the size in bytes . This result is real
			ioctl(fd, BLKGETSIZE, &blk);//just /512 of the BLKGETSIZE64
		}
		else{
			blk64=lseek(fd, 0, SEEK_END)+1;
			lseek(fd, 0, SEEK_SET)+1;
		}
		
		
		printf("I am db_flash.cc, flash_init, dbname=%s\n", dbname.c_str());
		printf("I am db_flash.cc, flash_init, blkpb=%llu, blklb=%llu,blk64=%llu,blk=%llu\n",blkpb,blklb,blk64,blk);
		printf("I am db_flash.cc, flash_init, device size=【%llu GB】\n",blk64/1024/1024/1024);
		//printf("page=%d B, block=%d MB, area=%d block\n",PAGE_BYTES,BLOCK_BYTES/1024/1024,AREA_BLOCKS);
		uint64_t pages_total=blk64/PAGE_BYTES;//所含的page数目	
		
		uint64_t bytes_offset=0;//偏移量
		//uint64_t info_addr=0;//addred by page
		//uint64_t level_entries_addr[12]={0};//one for level0, one for level1, two for level2 and 8 for level3
		
		printf("fff\n");
		this->segment_total=0; //总的段数目
		//this->db_dev=new std::string("sfd");;

		int fd_version=open(dbname.c_str(),O_RDWR);//针对MANIFEST	
		int fd_log=open(dbname.c_str(),O_RDWR);//针对LOG文件
		int fd_sst=open(dbname.c_str(),O_RDWR);//针对SSTABLE文件
		
		printf("db_flash.cc, fd_version=%d, fd_log=%d, fd_sst=%d\n", fd_version, fd_log,fd_sst);
		if(fd_version<0||fd_log<0||fd_sst<0){
			fprintf(stderr,"db_flash, open err,exit,9999\n");
			exit(9);
		}
		
		
		this->dev_fd_version=fd_version;
		this->dev_fd_log=fd_log;
		this->dev_fd_sst=fd_sst;
		
		this->db_name=dbname;
	
		this->segment_total=blk64/(SEGMENT_BLOCKS*BLOCK_BYTES);
        //总的段数目
		printf(" init. segment_total=%llu  \n",segment_total);

		SegTotal=segment_total;
		SegTable=(char *)malloc(segment_total); 
        //分配内存你，总段数目,肯定是bitmap
		memset(SegTable, 0, segment_total);
		
		int i;
		//unsigned int page_version[BLOCK_PAGES];
		//unsigned int current_page=0;
//give dev mmaped addr --begin
		bytes_offset=0;
		this->dev=( char*)mmap(NULL,segment_total*SEGMENT_BLOCKS*BLOCK_BYTES,PROT_WRITE,MAP_SHARED,fd,bytes_offset);
        //内存映射
		//this->dev_r=( char*)mmap(NULL,segment_total*SEGMENT_BLOCKS*BLOCK_BYTES,PROT_READ,MAP_SHARED,fd,bytes_offset);

		//printf(" I am Flash:init , segment_total*AREA_BLOCKS*BLOCK_BYTES=%llu\n", segment_total*AREA_BLOCKS*BLOCK_BYTES);
		printf("dev=%p\n",dev);
		
		printf("------------getpagesize()=%d\n",getpagesize() );
//give dev mmaped addr --end
//read the magic --begin
		files_name_block=( char*)mmap(NULL,BLOCK_BYTES,PROT_WRITE,MAP_SHARED,fd,bytes_offset);
		//应该是第一个段的映射


		char magic_signature[8]=MAGIC;
		//printf("mmaped files_name_block addr=%p\n",files_name_block);	
	
		int is_formated=-1;
		if(strncmp(magic_signature, (char *)files_name_block+14, MAGIC_BYTES)==0){ //a flashkv page 
			//已经格式化了	
				is_formated=1;
		}
	
		if(is_formated!=1){// the dev has been formated for flashkv, return
			//close(fd);
			//还没有格式化
			return 3; //3 says that the flash is not formated
		}
//read the magic --end
		//close(fd);
		//munmap(files_name_block,BLOCK_BYTES);//is formated, so this is not used
		printf("db_flash.cc, init end\n");
		return 1;
		
		
}
int Flash::set_valid(char* addr, uint64_t offset, unsigned char value){//value is 0, 1 or 2
	//unsigned char readin=*(unsigned char*)(addr+	offset);
	*(unsigned char*)(addr+	offset)=value;
    //回收时设备相应bitmap位为1	
	return 0;
	
}
int Flash::flash_format(const std::string& dbname){
        //格式化 
		printf("I am flash_format, mmaped files_name_block addr=%p\n",files_name_block);
		uint64_t conclussion_file_block_offset=5;
		uint64_t conclussion_file_entry_offset=5;
		//clear the block
		memset(files_name_block,0,BLOCK_BYTES);
		//printf("I am flash_format,00000\n");
		//set the default files number
		//*(uint32_t *)(files_name_block)=6;
//construct the conclusion entry --begin
		std::string file_name;
		file_name="Conclusion";
		//this->create_entry(file_name, conclussion_file_entry_offset,conclussion_file_block_offset);//at this point there is no free list, so the block_offset must be provided
	
//construct the conclusion entry --end
		
//set the conclusion file data --begin
		//char *conclusion_file=dev+conclussion_file_block_offset*BLOCK_BYTES;
		//memset(conclusion_file,0,BLOCK_BYTES);
		
		//set_valid(conclusion_file,0,1);//the first segment is in use
		//set_valid(conclusion_file,conclussion_file_block_offset,1);// the conclusion file segment is in use
		
//set the conclusion file data --end
//write the magic string --begin
		memcpy(files_name_block+14, MAGIC, MAGIC_BYTES);
        //魔数
//write the magic string --end

	//clear the menifest file --begin
		int manifest_seg_offset=2;
		char *manifest_file=dev + manifest_seg_offset*BLOCK_BYTES;
		memset(manifest_file, 0, BLOCK_BYTES );
        //manifest文件
		//printf("dev=%p\n",dev);
		//printf("manifest_file=%x\n",*manifest_file);
		//exit(9);
	//clear the menifest file --end
		
}

#define OPEN_FILE_ARG const std::string& fname, const char * mode
int Flash::flash_open(FLASH_OPEN_ARG){
	
	int static has_open=0;
	if(has_open){
		//printf("has opened, return \n");
		return 0;
	}

	printf("db_flash.cc, flash_open end\n");
	//exit(1);
	has_open=1;
	return 1;
	
	

}

}//end of leveldb
