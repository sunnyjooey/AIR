create table login
(
    username       varchar(32)   null,
    password       varchar(128)  null,
    teacher_id     int auto_increment
        primary key,
    login_attempts int default 0 not null
);

