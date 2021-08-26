show databases ;
use profile_tags;
show tables;
desc tbl_basic_tag;
show create table tbl_basic_tag;
CREATE DATABASE `profile_tags` DEFAULT CHARACTER SET utf8;
USE `profile_tags`;
-- ----------------------------
-- Table structure for tbl_basic_tag
-- ----------------------------
DROP TABLE IF EXISTS `tbl_basic_tag`;
CREATE TABLE `tbl_basic_tag` (
                                 `id` bigint(20) NOT NULL AUTO_INCREMENT,
                                 `name` varchar(50) DEFAULT NULL COMMENT '标签名称',
                                 `industry` varchar(30) DEFAULT NULL COMMENT '行业、子行业、业务类型、标签、属
性',
                                 `rule` varchar(300) DEFAULT NULL COMMENT '标签规则',
                                 `business` varchar(100) DEFAULT NULL COMMENT '业务描述',
                                 `level` int(11) DEFAULT NULL COMMENT '标签等级',
                                 `pid` bigint(20) DEFAULT NULL COMMENT '父标签ID',
                                 `ctime` datetime DEFAULT NULL COMMENT '创建时间',
                                 `utime` datetime DEFAULT NULL COMMENT '修改时间',
                                 `state` int(11) DEFAULT NULL COMMENT '状态：1申请中、2开发中、3开发完成、4已上
线、5已下线、6已禁用',
                                 `remark` varchar(100) DEFAULT NULL COMMENT '备注',
                                 PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=307 DEFAULT CHARSET=utf8 COMMENT='基础标签
表';
-- ----------------------------
-- Records of tbl_basic_tag
-- ----------------------------
INSERT INTO `tbl_basic_tag` VALUES ('307', '电商', null, null, null, '1',
                                    '-1', '2019-10-27 14:23:18', '2019-10-27 14:23:18', null, null);
INSERT INTO `tbl_basic_tag` VALUES ('308', '某商城', null, null, null, '2',
                                    '307', '2019-10-27 14:23:18', '2019-10-27 14:23:18', null, null);
INSERT INTO `tbl_basic_tag` VALUES ('309', '人口属性', null, null, null,
                                    '3', '308', '2019-11-29 22:14:24', '2019-11-29 22:14:24', null, null);
INSERT INTO `tbl_basic_tag` VALUES ('310', '商业属性', null, null, null,
                                    '3', '308', '2019-11-29 22:15:56', '2019-11-29 22:15:56', null, null);
INSERT INTO `tbl_basic_tag` VALUES ('311', '行为属性', null, null, null,
                                    '3', '308', '2019-11-29 22:16:13', '2019-11-29 22:16:13', null, null);
INSERT INTO `tbl_basic_tag` VALUES ('312', '用户价值', null, null, null,
                                    '3', '308', '2019-11-29 22:16:40', '2019-11-29 22:16:40', null, null);
-- ----------------------------
-- Table structure for tbl_model
-- ----------------------------
DROP TABLE IF EXISTS `tbl_model`;
CREATE TABLE `tbl_model` (
                             `id` bigint(20) NOT NULL AUTO_INCREMENT,
                             `tag_id` bigint(20) DEFAULT NULL,
                             `model_name` varchar(200) DEFAULT NULL,
                             `model_main` varchar(200) DEFAULT NULL,
                             `model_path` varchar(200) DEFAULT NULL,
                             `sche_time` varchar(200) DEFAULT NULL,
                             `ctime` datetime DEFAULT NULL,
                             `utime` datetime DEFAULT NULL,
                             `state` int(11) DEFAULT NULL,
                             `args` varchar(100) DEFAULT NULL,
                             PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;