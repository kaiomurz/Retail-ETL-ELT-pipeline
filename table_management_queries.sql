-- FACT TABLE
create table if not exists transactions (
    item_trans_id serial primary key,
    transaction_num int not null, -- Degenerate dimension
    item_num int not null, -- Degenerate dimension
    store_key int  not null,
    datetime_key char(10) not null,
    product_key int not null,
    quantity int not null,
    price numeric (4,2) not null,          
    constraint fk_store 
        foreign key(store_key)
        references stores(store_key),
    constraint fk_datetime 
        foreign key(datetime_key)    
        references datetimes(datetime_key),
    constraint fk_product
        foreign key(product_key)    
        references products(product_key)          
);


-- DIMENSIONAL TABLES

-- datetimes
create table if not exists datetimes (
    datetime_key char(10) primary key
    year int not null
    month int not null
    date int not null
    day_of_week char(3) not null
    hour int not null
);

-- stores
create table if not exists stores (
    store_key int primary key,
    city varchar(20) not null,
    address varchar(50) not null
    postal_code varchar(10) not null
    isd_code int(3) not null
    tel_no int(10) not null
);

insert into 
    stores(store_key, city, address, postal_code, isd_code, tel_no)
values
    (1, 'London', '49 Church Rd', 'SW13 9HH', 44, 2087410987),
    (2, 'Edinburgh', '109 The Royal Mile', 'EH1 1SG', 44, 1312203103),
    (3, 'Manchester', '23 Mosley St', 'M2 3JL', 44, 1612358888)
;

-- products
create table if not exists products(
    product_key serial primary key,
    flavour varchar(20) not null,
    size char(1) not null
);

insert into 
    products(product_key, flavour, variant)
values
    (1, 'banana', 'large'),
    (2, 'banana', 'medium'),
    (3, 'banana', 'small'),
    (4, 'coffee', 'large'),
    (5, 'coffee', 'medium'),
    (6, 'coffee', 'small'),
    (7, 'chocolate', 'large'),
    (8, 'chocolate', 'medium'),
    (9, 'chocolate', 'small'),
    (10, 'lemon', 'large'),
    (11, 'lemon', 'medium'),
    (12, 'lemon', 'small')
;
