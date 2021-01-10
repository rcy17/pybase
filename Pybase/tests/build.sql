DROP DATABASE DATASET;
CREATE DATABASE DATASET;
USE DATASET;

CREATE TABLE part (
    p_partkey       INT(10),
    p_name			VARCHAR(55),
    p_mfgr			VARCHAR(25),
    p_brand			VARCHAR(10),
    p_type			VARCHAR(25),
    p_size			INT(10),
    p_container		VARCHAR(10),
    p_retailpreice	FLOAT,
    p_comment       VARCHAR(23),

    PRIMARY KEY (p_partkey)
);

CREATE TABLE region (
	r_regionkey	    INT(10),
	r_name		    VARCHAR(25),
    r_comment       VARCHAR(152),

    PRIMARY KEY (r_regionkey)
);


CREATE TABLE nation (
	n_nationkey 	INT(10),
	n_name		    VARCHAR(25),
	n_regionkey	    INT(10) NOT NULL,
    n_comment       VARCHAR(152),

	PRIMARY KEY (n_nationkey)
);

CREATE TABLE supplier (
    s_suppkey	    INT(10),
    s_name		    VARCHAR(25),
    s_address	    VARCHAR(40),
    s_nationkey	    INT(10) NOT NULL,
    s_phone		    VARCHAR(15),
    s_acctbal	    FLOAT,
    s_comment       VARCHAR(101),

	PRIMARY KEY (s_suppkey),
	FOREIGN KEY (s_nationkey) REFERENCES nation(n_nationkey)
);

CREATE TABLE customer (
    c_custkey	    INT(10),
    c_name		    VARCHAR(25),
    c_address		VARCHAR(40),
    c_nationkey		INT(10) NOT NULL,
    c_phone			VARCHAR(15),
    c_acctbal		FLOAT,
    c_mktsegment	VARCHAR(10),
    c_comment       VARCHAR(117),

	PRIMARY KEY (c_custkey),
	FOREIGN KEY (c_nationkey) REFERENCES nation(n_nationkey)
);

CREATE TABLE partsupp (
    ps_partkey  	INT(10) NOT NULL,
    ps_suppkey	    INT(10) NOT NULL,
    ps_availqty	    INT(10),
    ps_supplycost  	FLOAT,
    ps_comment      VARCHAR(199),

    PRIMARY KEY (ps_partkey, ps_suppkey),
    FOREIGN KEY (ps_partkey) REFERENCES part(p_partkey),
    FOREIGN KEY (ps_suppkey) REFERENCES supplier(s_suppkey)
);

CREATE TABLE orders (
    o_orderkey	    INT(10),
    o_custkey	    INT(10) NOT NULL,
    o_orderstatus   VARCHAR(1),
    o_totalprice    FLOAT,
    o_orderdate	    DATE,
    o_orderpriority VARCHAR(15),
    o_clerk		    VARCHAR(15),
    o_shippriority  INT(10),
    o_comment       VARCHAR(79),

    PRIMARY KEY (o_orderkey),
    FOREIGN KEY (o_custkey) REFERENCES customer(c_custkey)
);

CREATE TABLE lineitem (
    l_orderkey		INT(10) NOT NULL,
    l_partkey		INT(10) NOT NULL,
    l_suppkey		INT(10) NOT NULL,
    l_linenumber	INT(10),
    l_quantity		FLOAT,
    l_extendedprice	FLOAT,
    l_discount		FLOAT,
    l_tax			FLOAT,
    l_returnflag	VARCHAR(1),
    l_linestatus	VARCHAR(1),
    l_shipdate		DATE,
    l_commitdate	DATE,
    l_receipdate	DATE,
    l_shipinstruct	VARCHAR(25),
    l_shipmode		VARCHAR(10),

    PRIMARY KEY (l_orderkey, l_linenumber),
    FOREIGN KEY (l_orderkey) REFERENCES orders(o_orderkey),
    FOREIGN KEY (l_partkey) REFERENCES part(p_partkey),
    FOREIGN KEY (l_suppkey) REFERENCES supplier(s_suppkey)
);

ALTER TABLE lineitem ADD CONSTRAINT L_PSKEY FOREIGN KEY (l_partkey,l_suppkey) REFERENCES partsupp(ps_partkey,ps_suppkey);
