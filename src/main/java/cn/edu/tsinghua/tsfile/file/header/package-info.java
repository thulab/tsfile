package cn.edu.tsinghua.tsfile.file.header;

/**
 RowGroupFooter and ChunkHeader are used for parsing file.

 RowGroupMetadata and ChunkMetadata are used for locating the positions of rowgroup (header) and chunk (header),
 filtering data quickly, and thereby they have digest information.

 However, because Page has only the header structure, therefore, PageHeader has the both the two functions.

 */