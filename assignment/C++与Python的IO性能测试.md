# C++与Python的IO性能测试

饶淙元 2019年10月16日

## 前言

布置数据库大作业时，我问助教如果用python完成是否会因为python本身的性能不如cpp而导致耗时过长，对此助教的回复是大作业的性能瓶颈主要在IO上，他对于python的IO性能表示怀疑。

在实测之前，我认为IO性能如何应该和操作系统以及我的硬件设备（SSD和机械硬盘可能有所不同？）有关，一个语言只要设计者不是过于愚蠢，应当底层都调用的类似的系统API，因此不应该存在显著差异。

而对于性能瓶颈在IO这件事情，基于我现在的数据库知识还无法做出判断，我确实担心最终因为B+树的执行效能等原因拖慢了时间，但是既然助教断言瓶颈在IO，那么最后真的因为python的CPU时间拖慢了效率，我是有底气取argue的。

## 预备

 在页式文件管理系统的PPT中提到使用”fopen()、fclose()、read()、write() “等函数，对此我表示怀疑，f系函数应当一起使用，fopen后理应是fread、fwrite，而read、write这些linux函数应当和open、close合用。为此，我看了大作业提供的源码中的文件管理部分，果然找到了如下部分：

```C++
/*
* file: filesystem/fileio/FileManager.h
* liens: 18~34
*/
int _createFile(const char* name) {
    FILE* f = fopen(name, "a+");
    if (f == NULL) {
        cout << "fail" << endl;
        return -1;
    }
    fclose(f);
    return 0;
}
int _openFile(const char* name, int fileID) {
    int f = open(name, O_RDWR);
    if (f == -1) {
        return -1;
    }
    fd[fileID] = f;
    return 0;
}
```

```C++
/*
* file: filesystem/fileio/FileManager.h
* liens: 52~63
*/
int writePage(int fileID, int pageID, BufType buf, int off) {
    int f = fd[fileID];
    off_t offset = pageID;
    offset = (offset << PAGE_SIZE_IDX);
    off_t error = lseek(f, offset, SEEK_SET);
    if (error != offset) {
        return -1;
    }
    BufType b = buf + off;
    error = write(f, (void*) b, PAGE_SIZE);
    return 0;
}
```

readPage与writePage同理。可以看到的是，在创建新文件的接口里使用了fopen、fclose的接口，但是此外的页式文件操作里均用了open、close、read、write这套函数，因此实际上这份代码是基于linux的若干头文件开发的，由于我对linux开发不熟，不是很清楚哪些函数、宏在哪些头文件里，只知道大概和如下头文件中的一个或多个有关系：

- sys/types.h
- sys/stat.h
- unistd.h
- fcntl.h

无独有偶，python中也存在基于文件IO类的文件操作接口，与基于os库id传值的文件操作接口，分别与上面两种方法类似，为此我分别做了测试。

## 测试

测试的设计非常简单，我准备了一个1.4G的大文件，测试逻辑是用可读可写的方法打开该文件，然后把8192字节看作一页，以页为操作单位将[1, 1024 * 64]页分别往前”搬动“一页（即涉及到512MB文件的读与写）。为了便于写程序，这里用简单的for循环微调控制文件指针的位置，seek的性能并不在测试考虑范围之内。

#### C++

首先测试windows上的表现，代码如下：

```C++
#include <stdio.h>
#include <time.h>
#pragma warning(disable:4996)

int main()
{
	long start = clock();
	FILE* file;
	const int page_size = 8192;
	char data[page_size];
	int total = 1024 * 64;
	file = fopen("data_in", "rb+");
	if (!file)
	{
		printf("error!");
		return 0;
	}
	for (int i = 0; i < total; i++)
	{
		fseek(file, page_size, SEEK_CUR);
		fread(data, 1, page_size, file);
		fseek(file, -2 * page_size, SEEK_CUR);
		fwrite(data, 1, page_size, file);
	}
	fclose(file);
	long stop = clock();
	printf("cost %.3f s in total\n", float(stop - start) / CLOCKS_PER_SEC);
	return 0;
}
```

此处开发环境是VS2019，默认开启了O2优化的release模式，运行五次记录输出的时间如下：

| 实验次数 | 时间(s) |
| -------- | ------- |
| 1        | 5.700   |
| 2        | 6.141   |
| 3        | 5.961   |
| 4        | 4.960   |
| 5        | 5.764   |

（注：在我的电脑上，除了linux比较稳定外，用VS和python经常都会出现第一次运行较慢，后面变快的情况，因此记录时不记录显著慢于其他的那一次。）

同样的代码我在wsl上用`g++ main.cpp -o main.out`编译运行，用time作为时间度量标准（经查clock在linux下不会记录IO时间），记录如下：

| 实验次数 | 时间(s) |
| -------- | ------- |
| 1        | 5.285   |
| 2        | 7.290   |
| 3        | 6.789   |
| 4        | 6.638   |
| 5        | 6.471   |

这个结果和VS里跑的相差不大，至少是一个量级的。

然后是包含了那些仅在linux上work的代码的非f系API代码：

```C++
#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

int main()
{
	const int page_size = 8192;
	char data[page_size];
	int total = 1024 * 64;
	int file = open("data_in", O_RDWR);
	
	for (int i = 0; i < total; i++)
	{
		lseek(file, page_size, SEEK_CUR);
		read(file, (void *) data, page_size);
		lseek(file, -2 * page_size, SEEK_CUR);
		write(file, (void *) data, page_size);
	}
	close(file);
	return 0;
}
```

除了改用接口外基本没有什么改变，然后来测试性能如下：

| 实验次数 | 时间(s) |
| -------- | ------- |
| 1        | 5.145   |
| 2        | 5.680   |
| 3        | 8.803   |
| 4        | 6.484   |
| 5        | 6.809   |

可以看到性能甚至比在windows上还差点，不知道是不是因为我用的wsl而非真正的linux的缘故。

### Python

首先用python自带的open和close函数(对应C++的fopen和fclose的用法)：

```python
from datetime import datetime
total = 1024 * 64
page_size = 8192
SEEK_SET = 0
SEEK_CUR = 1
SEEK_END = 2

start = datetime.now()

file = open('data_in', 'rb+')

for i in range(total):
    file.seek(page_size, SEEK_CUR)
    data = file.read(page_size)
    file.seek(-2 * page_size, SEEK_CUR)
    file.write(data)

file.close()

stop = datetime.now()
print(f"cost {(stop - start).total_seconds():.3f} s in total")
```

在windows上测试结果如下：

| 实验次数 | 时间(s) |
| -------- | ------- |
| 1        | 4.679   |
| 2        | 5.206   |
| 3        | 5.911   |
| 4        | 6.543   |
| 5        | 6.103   |

这表现和windows上的c++如出一辙，顺便我也用wsl里运行过，结果无显著差异，不再赘述。

再看用os系的函数调用(对应linux的C++头文件操作)：

```python
from datetime import datetime
import os
total = 1024 * 64
page_size = 8192
SEEK_SET = 0
SEEK_CUR = 1
SEEK_END = 2

start = datetime.now()

file = os.open('data_in', os.O_RDWR)

for i in range(total):
    os.lseek(file, page_size, SEEK_CUR)
    data = os.read(file, page_size)
    os.lseek(file, -2 * page_size, SEEK_CUR)
    os.write(file, data)

os.close(file)

stop = datetime.now()
print(f"cost {(stop - start).total_seconds():.3f} s in total")
```

测试结果如下：

| 实验次数 | 时间(s) |
| -------- | ------- |
| 1        | 0.530   |
| 2        | 0.604   |
| 3        | 0.499   |
| 4        | 0.588   |
| 5        | 0.556   |

Amazing！没想到python用os库的IO可以到这么快，虽然我不理解其原因，但至少表现是非常好了，拿来做大作业效率甚至可能反超C++。

再看看在wsl里运行这段代码的效率：

| 实验次数 | 时间(s) |
| -------- | ------- |
| 1        | 4.616   |
| 2        | 4.889   |
| 3        | 4.616   |
| 4        | 6.901   |
| 5        | 6.213   |

可以看出性能大概会比C++好一些，但是运行到后面几次又变慢了，这或许与我电脑的状态有一定关系？但是结论基本已经出来，不做深究。

需要说明的是以上测试是假定每次两页的seek不会带来显著的性能差异，seek进行长距离偏移时的性能我没有做更多测试，只通过少量的测试得出了”seek用的时间与seek偏移距离成正比“的结论，这里也不做赘述。

## 结论

根据上述实验发现python普通的文件操作不会比windows上的C++文件操作慢，python用os库的文件操作在windows上速度比C++快得多。当然这个结论我自己也认为存疑，python测出来快得有些”异常“，且在windows和wsl有如此之大的差别，我不确定是不是什么步骤被优化掉了或者我代码写错了。

但总的来说，就算上面我的担心都成为现实，我也能根据靠谱的5s左右的实验数据表示"python的文件IO不比C++慢"，如果助教说的时间瓶颈主要在文件这一点没有问题的话，那我用python写数据库大作业也应该是没问题的。