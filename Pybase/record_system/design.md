# Record System

记录管理系统模式主要参考 cs346 的设计架构完成，设计细节上采用了第一页为文件头，后面页或为数据记录页，或为varchar记录页。
记录用了定长模式，varchar这样的变长数据进行单独存储

## Header

每个header中会存有部分关于记录的结构，直接使用json进行记录。
记录信息如下：

- record_length：int, 表示一条记录的固定长度；

- record_per_page：int, 表示每页的记录数；

- page_number：int, 表示该表总页数；

- record_number：int, 表示该表记录总数；

- next_vacancy_page：int, 表示下一个有空位页的页码;

- bitmap_length: int, 表示每页位图所占大小；

## 参考资料

 谷歌“how variable-length records deal with delete”后找到的文章 RECLAIMING SPACE IN FILES ：https://www.site.uottawa.ca/~lucia/courses/2131-02/lect09.pdf 

| 1    | $|Z|(\Omega)$ | \|   |
| ---- | ------------- | ---- |
|      |               |      |
|      |               |      |
|      |               |      |

