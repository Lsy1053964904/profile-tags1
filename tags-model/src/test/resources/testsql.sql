SELECT id, name, rule, level  FROM tbl_basic_tag WHERE id = 318
UNION
SELECT id, name, rule, level  FROM tbl_basic_tag WHERE pid = 318;

select * from profile_tags.tbl_model limit 5;
#job` varchar(60) DEFAULT NULL COMMENT '职业；1学生、2公务员、3军人、4警察、5教师、6白领',
select job ,count(1)  as total_count from tags_dat.tbl_users group by job;
show create table tags_dat.tbl_users;

SELECT id, name, rule, level  FROM profile_tags.tbl_basic_tag WHERE id = 321
UNION
SELECT id, name, rule, level  FROM profile_tags.tbl_basic_tag WHERE pid = 321;

# `politicalFace` int(1) unsigned DEFAULT NULL COMMENT '政治面貌：1群众、2党员、3无党派人士',
select politicalFace ,count(1)  as total_count from tags_dat.tbl_users group by politicalFace;

show databases ;
use tags_dat;
show tables;


show create table  tbl_orders;

select memberId,finishTime from tbl_tag_orders limit 10;
show tables;
select memberId,paymentCode  ,count(memberId) ,count(paymentCode) from tbl_tag_orders group by memberId ,paymentCode order by memberId ;

select  memberId,paymentCode from  tbl_tag_orders  order by memberId asc Limit 30;





show tables;
show databases ;