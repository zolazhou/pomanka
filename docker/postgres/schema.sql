
create table topics
(
	name varchar(100) not null
		constraint topics_pk
			primary key,
	partitions int default 1 not null
);

alter table topics
    owner to pomanka;

create table offsets
(
    consumer  varchar(100) not null,
    topic     varchar(100) not null,
    partition integer      not null,
    value     bigint       not null,
    constraint offsets_pk_2
        primary key (consumer, topic, partition)
);

alter table offsets
    owner to pomanka;


-- bottlewater tables:

create table bw_offsets
(
    key   bytea not null
        constraint bw_offsets_pk
            primary key,
    value bytea
);

alter table bw_offsets
    owner to pomanka;


create table bw_schemas
(
	id serial
		constraint bw_schemas_pk
			primary key,
	sha text not null,
	schema text not null
);

alter table bw_schemas
    owner to pomanka;


create schema topics;

alter schema topics
    owner to pomanka;
