select sum(bytes)/1024/1024/1024 as size_GB from dba_segments where owner like 'CMS_DBS3%';
