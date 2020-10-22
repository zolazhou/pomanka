create database sakura;

\c sakura;

create table memberships
(
    id          bigserial                              not null primary key,
    user_id     bigint                                 not null,
    type        varchar(12)                            not null,
    space_usage bigint                   default 0     not null,
    created_at  timestamp with time zone default now() not null,
    expires_at  timestamp with time zone
);

alter table memberships
    owner to pomanka;

create unique index memberships_user_id_uindex
    on memberships (user_id);

alter table memberships replica identity full;

create table directories
(
    id         bigint                                 not null,
    user_id    bigint                                 not null,
    parent_id  bigint,
    name       text                                   not null,
    created_at timestamp with time zone default now() not null,
    updated_at timestamp with time zone default now() not null,
    revision   integer                  default 1     not null,
    cover_id   bigint,
    seq        integer                  default 0     not null
);

alter table directories
    owner to pomanka;

alter table directories
    add constraint directories_pk
        primary key (user_id, id);

create unique index directories_user_id_parent_id_name_uindex
    on directories (user_id, parent_id, name);

alter table directories replica identity full;

