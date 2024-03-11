IF OBJECT_ID(N'product', N'U') IS NULL
CREATE TABLE product
(
	type varchar,
	line varchar,
	introduction_date date,
	price decimal,
	margin decimal,
	language varchar,
	product_number int,
	name varchar,
	image varchar,
	primary key (product_number)
);
GO

IF OBJECT_ID(N'sales_product_forecast', N'U') IS NULL
CREATE TABLE sales_product_forecast
(
	nr int,
	year int,
	month int,
	expected_volume int,
	unit_price decimal,
	turnover decimal,
	introduction_date date,
	months_since_release int,
	years_since_release int,
	product_number int foreign key references product(product_number),
	primary key (nr)
);
GO

IF OBJECT_ID(N'sales_inventory_levels', N'U') IS NULL
CREATE TABLE sales_inventory_levels
(
	id int,
	year int,
	month int,
	count int,
	cost decimal,
	amount_sold_month int,
	amount_sold_year int,
	margin decimal,
	excess_amount int,
	product_number int foreign key references product(product_number),
	primary key(id)
);
GO

IF OBJECT_ID(N'sales_staff', N'U') IS NULL
CREATE TABLE sales_staff
(
	sales_staff_code int,
	f_name varchar,
	l_name varchar,
	phone varchar,
	email varchar,
	fax varchar,
	sales_branch_code int,
	country_code int
	primary key (sales_staff_code)
);
GO

IF OBJECT_ID(N'sales_targetdata', N'U') IS NULL
CREATE TABLE sales_targetdata
(
	target_id int,
	target_product_nr int,
	target_staff_code int,
	target_sales_year int,
	target_sales_period int,
	target_sales_total_target_period int,
	target_sales_total_target_year int,
	target_sales_cost int,
	target_sales_margin int,
	target_Sales_target_profit int,
	sales_staff_id int foreign key references sales_staff(sales_staff_code),
	product_id int foreign key references product(product_number),
	primary key (target_id)
);
GO

IF OBJECT_ID(N'course', N'U') IS NULL
CREATE TABLE course
(
	course_code int,
	course_description varchar,
	primary key (course_code)
);
GO

IF OBJECT_ID(N'training', N'U') IS NULL
CREATE TABLE training
(
	training_id int,
	training_course_id int foreign key references course(course_code),
	training_staff_id int foreign key references sales_staff(sales_staff_code),
	training_amount_enrolled_course int,
	training_amount_enrolled_staff int,
	training_country varchar,
	training_amount_enrolled_course_country int,
	training_amount_enrolled_staff_country int,
	primary key (training_id)
);
GO

IF OBJECT_ID(N'satisfaction_type', N'U') IS NULL
CREATE TABLE satisfaction_type
(
	satisfaction_type_code int,
	description varchar,
	primary key (satisfaction_type_code)
);
GO

IF OBJECT_ID(N'satisfaction', N'U') IS NULL
CREATE TABLE satisfaction
(
	satisfaction_id int,
	satisfaction_type_id int foreign key references satisfaction_type(satisfaction_type_code),
	satisfaction_staff_id int foreign key references sales_staff(sales_staff_code),
	satisfaction_amount_staff int,
	satisfaction_country_percent decimal,
	satisfaction_position_percent decimal,
	primary key (satisfaction_id)
);
GO

IF OBJECT_ID(N'returned_reason', N'U') IS NULL
CREATE TABLE returned_reason
(
	return_reason_code int,
	description varchar,
	primary key(return_reason_code)
);
GO

IF OBJECT_ID(N'returned_item', N'U') IS NULL
CREATE TABLE returned_item
(
	return_code int,
	returned_item_reason_id int,
	returned_item_description varchar,
	returned_reason_code int foreign key references returned_reason(return_reason_code),
	primary key (return_code)
);
GO

IF OBJECT_ID(N'retailer_contact', N'U') IS NULL
CREATE TABLE retailer_contact
(
	sales_territory_id int,
	sales_territory_name_en varchar,
	country_id int,
	country_en varchar,
	country_flag_image varchar,
	country_sales_territory_code int,
	retailer_site_code int,
	retailer_site_address1 varchar,
	retailer_site_address2 varchar,
	retailer_site_city varchar,
	retailer_site_region varchar,
	retailer_site_country_code int,
	retailer_site_active_indicator int,
	retailer_headquarter_codenr int,
	retailer_headquarter_name varchar,
	retailer_headquarter_address1 varchar,
	retailer_headquarter_address2 varchar,
	retailer_headquarter_city varchar,
	retailer_headquarter_region varchar,
	retailer_headquarter_country_code int,
	retailer_headquarter_active_indicator int,
	retailer_type_code int,
	retailer_type_type_en varchar,
	retailer_segment_code int,
	retailer_segment_language varchar,
	retailer_segment_name varchar,
	retailer_segment_description varchar,
	retailer_contact_code int,
	retailer_contact_site_code int,
	retailer_contact_first_name varchar,
	retailer_contact_last_name varchar,
	retailer_contact_fax varchar,
	retailer_contact_email varchar,
	retailer_contact_active_indicator int,
	retailer_contact_retailer_id int,
	retailer_contact_retailer_codemr int,
	retailer_contact_retailer_name varchar,
	retailer_contact_retailer_type_code int,
	retailer_contact_jobposition_en varchar,
	retailer_contact_extension varchar,
	retailer_contact_gender varchar,
	retailer_contact_age_group_code int,
	retailer_contact_age_group_upper int,
	retailer_contact_age_group_lower int,
	retailer_contact_sales_demographic_code int,
	retailer_contact_sales_demographic_codemr int,
	retailer_contact_sales_demographic_age_group_code int,
	retailer_contact_sales_demographic_sales_percent decimal,
	primary key (sales_territory_id, country_id, retailer_site_code, retailer_headquarter_codenr, retailer_type_code, retailer_segment_code, retailer_contact_code, retailer_contact_retailer_id, retailer_contact_age_group_code, retailer_contact_sales_demographic_code, retailer_contact_sales_demographic_codemr)
);
GO

IF OBJECT_ID(N'order_method', N'U') IS NULL
CREATE TABLE order_method
(
	order_method_code int,
	order_method_en varchar,
	primary key (order_method_code)
);
GO

IF OBJECT_ID(N'order', N'U') IS NULL
CREATE TABLE order
(
	order_details_code int,
	order_product_number int,
	order_staff_code int,
	order_method_code int,
	order_retailer_code int,
	order_quantity int,
	order_unit_cost decimal,
	order_unit_sale decimal,
	order_product_margin decimal,
	order_product_cost decimal,
	order_total_price_before_sale decimal,
	order_sale_amount int,
	order_total_price_after_sale decimal,
	order_total_profit decimal,
	order_single_profit_margin decimal,
	order_method_id int foreign key references order_method(order_method_code),
	retailer_site_id int,
	sales_staff_id int foreign key references sales_staff(sales_staff_code),
	product_id int foreign key references product(product_number),
	primary key(order_details_code)
);
GO
