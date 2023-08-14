DROP SCHEMA IF EXISTS Warehouse_Key_DRM CASCADE;

CREATE SCHEMA Warehouse_Key_DRM;

CREATE TABLE IF NOT EXISTS Warehouse_Key_DRM.TVOD(        
	ID varchar(255) not null,
	BHD_Total_Key bigint, 
	Fimplus_Total_Key bigint,
	TVOD_Total_Key bigint,
	Date varchar(255)
);

CREATE TABLE IF NOT EXISTS Warehouse_Key_DRM.SVOD(        
	ID varchar(255) not null,
	SVOD_Total_Key bigint, 
	Date varchar(255)
);

CREATE TABLE IF NOT EXISTS Warehouse_Key_DRM.Time(        
	Date varchar(255) not null,
	Day varchar(255), 
	Month varchar(255),
	Year varchar(255)
);

CREATE TABLE IF NOT EXISTS Warehouse_Key_DRM.Customers(         
	CustomerID varchar(255) not null,
	Mac varchar(255),
	Created_date varchar(255)
);

CREATE TABLE IF NOT EXISTS Warehouse_Key_DRM.CustomerService(    
	CustomerID varchar(255) not null,
	ServiceID varchar(255),
	Amount int,
	Date varchar(255)
);

CREATE TABLE IF NOT EXISTS Warehouse_Key_DRM.Log_Get_DRM_List(        
	CustomerID varchar(255) not null,
	Date varchar(255),
	Mac varchar(255)
);

CREATE TABLE IF NOT EXISTS Warehouse_Key_DRM.Log_BHD_MovieID(    
	CustomerID varchar(255) not null,
	MovieID varchar(255) not null,
	RealTimePlaying int,
	Date varchar(255)
);

CREATE TABLE IF NOT EXISTS Warehouse_Key_DRM.Log_Fimplus_MovieID(  
	CustomerID varchar(255) not null,
	MovieID varchar(255) not null,
	RealTimePlaying int,
	Date varchar(255)
);

CREATE TABLE IF NOT EXISTS Warehouse_Key_DRM.MV_PropertiesShowVN(    
	ID varchar(255) not null,
	TopTitle varchar(255),
	TitleEN varchar(255),
	Release varchar(4),
	Actors varchar(255),
	Directors varchar(255),
	Producers varchar(255),
	PublishCountry varchar(255),
	Duration int,
	isDRM bool
);



