# Record System

记录管理系统模式主要参考 cs346 的设计架构完成，设计细节上采用了第一页为文件头，后面页或为数据记录页，或为varchar记录页。
记录用了定长模式，varchar这样的变长数据进行单独存储

## Header

每个header中会存有部分关于记录的结构，其具体实现主要借助了protobuf直接序列化相关信息以减少工作量。
记录信息如下：

1. record_length：表示一条记录的固定长度；

2. record_per_page：表示每页的记录数；

3. page_number：表示该表总页数；

4. record_number：表示该表记录总数；

5. next_vacancy_page：表示下一个有空位页的页码;

## 参考资料

 谷歌“how variable-length records deal with delete”后找到的文章 RECLAIMING SPACE IN FILES ：https://www.site.uottawa.ca/~lucia/courses/2131-02/lect09.pdf 

| 1    | $|Z|(\Omega)$ | \|   |
| ---- | ------------- | ---- |
|      |               |      |
|      |               |      |
|      |               |      |

