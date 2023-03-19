--DDL for the relational database
--EERST DE VARIABELEN AANMAKEN DAN PAS FOREIGN KEY

create table product(
    _id varchar primary key,
    brand varchar(255),
    category varchar(255),
    colour varchar(255),
    fast_mover boolean,
    gender varchar(255),
    product_name varchar(255) NOT NULL,
    price integer NOT NULL,
    discount boolean,
    target_group varchar(255),
    size varchar(255),
    age varchar(255),
    series varchar(255),
    product_type varchar(255),
    sub_category varchar(255),
    sub_sub_category varchar(255),
    sub_sub_sub_category varchar(255)
);

create table user_profile(
    _id char(24) primary key
);

create table user_session(
    _id varchar(255) primary key,
    preference_brand varchar(255),
    preference_category varchar(255),
    preference_gender varchar(255),
    preference_sub_category varchar(255),
    preference_sub_sub_category varchar(255),
    preference_promos varchar(255),
    preference_product_type varchar(255),
    preference_product_size varchar(255)
);

create table prev_recommended(
    user_profile_id char(24),
    product_id VARCHAR(255),
    foreign key (user_profile_id) references user_profile(_id),
    foreign key (product_id) references product(_id)
);

create table session_order(
    product_id char(30),
    session_id varchar(255),
    foreign key (product_id) references product(_id),
    foreign key (session_id) references user_session(_id)
);

create table buid(
    _id varchar(255),
    user_profile_id char(24),
    user_session_id char(50),
    foreign key (user_profile_id) references user_profile(_id),
    foreign key (user_session_id) references user_session(_id)
);
